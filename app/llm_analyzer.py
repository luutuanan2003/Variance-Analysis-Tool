import json
import io
from typing import List, Dict, Any
import pandas as pd
import ollama


class LLMFinancialAnalyzer:
    def __init__(self, model_name: str = "llama3.1"):
        """Initialize LLM analyzer with Ollama model."""
        self.model_name = model_name

    # ===========================
    # Generation option profiles
    # ===========================
    def _max_ollama_options(self):
        """
        Maximum performance configuration using all available hardware resources.
        Optimized for comprehensive analysis with hardware acceleration.
        """
        return {
            "temperature": 0.15,   # Keep quality consistent
            "top_p": 0.9,
            "repeat_penalty": 1.1,
            "num_ctx": 200_000,    # Large context for thorough analysis
            "num_predict": 32_768, # Large output for comprehensive analysis of big files
            "num_keep": 1024,      # Keep system prompt in memory
            "num_thread": -1,      # Use ALL available CPU threads
            "num_gpu": -1,         # Use ALL available GPU layers
            "num_batch": 512,      # Larger batch size for GPU efficiency
            "use_mlock": True,     # Lock model in memory to avoid swapping
            "use_mmap": True,      # Memory map for faster loading
        }

    def _reduced_ollama_options(self, level=1):
        """Automatic fallback profiles if OOM/timeout or server errors occur."""
        profiles = [
            {"num_ctx": 128_000, "num_predict": 20_480, "num_thread": -1, "num_gpu": -1, "num_batch": 256},
            {"num_ctx": 64_000,  "num_predict": 16_384, "num_thread": -1, "num_gpu": -1, "num_batch": 128},
            {"num_ctx": 32_000,  "num_predict": 12_288, "num_thread": -1, "num_gpu": -1, "num_batch": 64},
        ]
        base = {
            "temperature": 0.15,
            "top_p": 0.9,
            "repeat_penalty": 1.1,
            "use_mlock": True,
            "use_mmap": True
        }
        p = profiles[min(level, len(profiles)-1)]
        base.update(p)
        return base

    # ===========================
    # Main entrypoints
    # ===========================
    def analyze_raw_excel_file(
        self,
        excel_bytes: bytes,
        filename: str,
        subsidiary: str,
        config: dict
    ) -> List[Dict[str, Any]]:
        """Analyze raw Excel file focusing on BS Breakdown and PL Breakdown sheets."""
        print(f"\n🔍 ===== STARTING RAW EXCEL ANALYSIS FOR {subsidiary} =====")
        print(f"📄 File: {filename}")
        print(f"📏 File Size: {len(excel_bytes):,} bytes ({len(excel_bytes)/1024:.1f} KB)")
        print(f"🤖 Model: {self.model_name}")

        try:
            print(f"\n📋 STEP 1: Loading Raw Excel Sheets")
            print(f"   🔄 Reading 'BS Breakdown' sheet...")

            # Read BS Breakdown sheet completely raw
            bs_raw = pd.read_excel(io.BytesIO(excel_bytes), sheet_name="BS Breakdown", header=None, dtype=str)
            print(f"   ✅ BS Breakdown loaded: {len(bs_raw)} rows, {len(bs_raw.columns)} columns")

            print(f"   🔄 Reading 'PL Breakdown' sheet...")
            # Read PL Breakdown sheet completely raw
            pl_raw = pd.read_excel(io.BytesIO(excel_bytes), sheet_name="PL Breakdown", header=None, dtype=str)
            print(f"   ✅ PL Breakdown loaded: {len(pl_raw)} rows, {len(pl_raw.columns)} columns")

            print(f"\n📝 STEP 2: Converting to CSV for AI Analysis")
            print(f"   🔄 Converting raw Excel data to CSV format...")

            # Convert raw DataFrames to CSV strings
            bs_csv = bs_raw.to_csv(index=False, header=False)  # No headers, pure raw data
            pl_csv = pl_raw.to_csv(index=False, header=False)  # No headers, pure raw data

            print(f"   ✅ CSV conversion complete:")
            print(f"      • BS CSV: {len(bs_csv):,} characters")
            print(f"      • PL CSV: {len(pl_csv):,} characters")

            print(f"\n📝 STEP 3: Creating AI Analysis Prompt")
            prompt = self._create_raw_excel_prompt(bs_csv, pl_csv, subsidiary, filename, config)
            prompt_length = len(prompt)
            estimated_tokens = prompt_length // 4
            print(f"   ✅ Prompt generation complete:")
            print(f"      • Total prompt length: {prompt_length:,} characters")
            print(f"      • Estimated input tokens: {estimated_tokens:,}")

            print(f"\n🤖 STEP 4: AI Model Processing")
            response = None
            options = None
            attempt = 1

            try:
                print(f"   🚀 Attempt {attempt}: Maximum performance configuration")
                options = self._max_ollama_options()
                print(f"      • Context window: {options['num_ctx']:,} tokens")
                print(f"      • Max output: {options['num_predict']:,} tokens")
                print(f"   🔄 Sending complete raw Excel data to AI...")

                response = ollama.chat(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": self._get_raw_excel_system_prompt()},
                        {"role": "user", "content": prompt}
                    ],
                    options=options
                )

                # Extract token usage information if available
                if response and 'prompt_eval_count' in response:
                    input_tokens = response.get('prompt_eval_count', 0)
                    output_tokens = response.get('eval_count', 0)
                    total_tokens = input_tokens + output_tokens
                    print(f"   📊 Token Usage:")
                    print(f"      • Input tokens: {input_tokens:,}")
                    print(f"      • Output tokens: {output_tokens:,}")
                    print(f"      • Total tokens: {total_tokens:,}")

                print(f"   ✅ AI analysis successful on attempt {attempt}")

            except Exception as e:
                print(f"   ❌ AI analysis failed: {str(e)}")
                return [{
                    "subsidiary": subsidiary,
                    "account_code": "SYSTEM_ERROR",
                    "rule_name": "AI Analysis Error",
                    "description": f"Raw Excel AI analysis failed: {str(e)[:100]}...",
                    "details": f"Error processing raw Excel file: {str(e)}",
                    "current_value": 0,
                    "previous_value": 0,
                    "change_amount": 0,
                    "change_percent": 0,
                    "severity": "High",
                    "sheet_type": "Error"
                }]

            print(f"\n📄 STEP 5: Processing AI Response")
            if not response or 'message' not in response or not response['message'] or 'content' not in response['message']:
                print(f"   ❌ Invalid response structure from Ollama")
                raise RuntimeError("Empty response payload from Ollama")

            result = response['message']['content'] or ""
            response_length = len(result)

            # Extract final token usage from successful response
            total_input_tokens = response.get('prompt_eval_count', 0)
            total_output_tokens = response.get('eval_count', 0)
            total_tokens_used = total_input_tokens + total_output_tokens

            print(f"   ✅ Response received successfully:")
            print(f"      • Response length: {response_length:,} characters")
            if total_tokens_used > 0:
                print(f"   💰 FINAL TOKEN SUMMARY:")
                print(f"      • Total Input Tokens: {total_input_tokens:,}")
                print(f"      • Total Output Tokens: {total_output_tokens:,}")
                print(f"      • TOTAL TOKENS USED: {total_tokens_used:,}")
                print(f"      • Model: {self.model_name}")
                print(f"      • Using Ollama (Local): FREE ✅")

            print(f"   📝 Response preview: {result[:200]}...")

            # Debug: Print the full AI response
            print(f"\n📄 ===== FULL AI RESPONSE =====")
            print(result)
            print(f"===== END AI RESPONSE =====\n")

            print(f"\n🔍 STEP 6: JSON Parsing & Validation")
            anomalies = self._parse_llm_response(result, subsidiary)

            print(f"   ✅ Parsing completed successfully:")
            print(f"      • Anomalies detected: {len(anomalies)}")

            print(f"\n🎉 ===== RAW EXCEL AI ANALYSIS COMPLETE FOR {subsidiary} =====")
            print(f"📊 Final Results: {len(anomalies)} anomalies identified")
            if total_tokens_used > 0:
                print(f"🔢 Processing Summary: {total_tokens_used:,} tokens used (FREE with Ollama)")
            print()
            return anomalies

        except Exception as e:
            print(f"\n❌ Raw Excel analysis failed for '{subsidiary}': {e}")
            return [{
                "subsidiary": subsidiary,
                "account_code": "SYSTEM_ERROR",
                "rule_name": "Raw Excel Analysis Failed",
                "description": f"Failed to analyze raw Excel file: {str(e)[:100]}...",
                "details": f"Raw Excel analysis error: {str(e)}",
                "current_value": 0,
                "previous_value": 0,
                "change_amount": 0,
                "change_percent": 0,
                "severity": "High",
                "sheet_type": "Error"
            }]

    def analyze_financial_data(
        self,
        bs_df: pd.DataFrame,
        pl_df: pd.DataFrame,
        subsidiary: str,
        config: dict
    ) -> List[Dict[str, Any]]:
        print(f"\n🔍 ===== STARTING AI ANALYSIS FOR {subsidiary} =====")
        print(f"📊 Input Data Validation:")
        print(f"   • Balance Sheet: {len(bs_df)} rows, {len(bs_df.columns)} columns")
        print(f"   • Profit & Loss: {len(pl_df)} rows, {len(pl_df.columns)} columns")
        print(f"   • Model: {self.model_name}")

        # Quick sanity checks (both sheets should be non-empty by the time we get here)
        if pl_df is None or pl_df.empty:
            print("❌ ERROR: Profit & Loss data is empty or None")
            raise ValueError("Profit & Loss data is empty or None")
        if bs_df is None or bs_df.empty:
            print("❌ ERROR: Balance Sheet data is empty or None")
            raise ValueError("Balance Sheet data is empty or None")

        """
        Analyze financial data using LLaMA to detect anomalies and provide explanations.
        Returns a list of anomaly dictionaries.
        """
        # Step 1: Convert DataFrames to simple CSV format for AI
        print(f"\n📋 STEP 1: Raw Data Preparation")
        print(f"   🔄 Converting Excel data to CSV format for AI analysis...")

        # Convert to simple CSV strings that AI can easily read
        bs_csv = bs_df.to_csv(index=False)
        pl_csv = pl_df.to_csv(index=False)

        print(f"   ✅ Data conversion complete:")
        print(f"      • Balance Sheet: {len(bs_df)} rows, {len(bs_df.columns)} columns")
        print(f"      • P&L: {len(pl_df)} rows, {len(pl_df.columns)} columns")
        print(f"      • Full raw data passed to AI for comprehensive analysis")

        # Step 2: Create analysis prompt with raw data
        print(f"\n📝 STEP 2: Prompt Generation")
        print(f"   🔄 Building AI analysis prompt with full Excel data...")
        prompt = self._create_raw_data_prompt(bs_csv, pl_csv, subsidiary, config)
        prompt_length = len(prompt)
        estimated_tokens = prompt_length // 4  # Rough estimate: 4 chars per token
        print(f"   ✅ Prompt generation complete:")
        print(f"      • Prompt length: {prompt_length:,} characters")
        print(f"      • Estimated input tokens: {estimated_tokens:,}")

        # Step 3: AI Model Processing with Fallback Strategy
        print(f"\n🤖 STEP 3: AI Model Processing")
        response = None
        options = None
        attempt = 1

        try:
            print(f"   🚀 Attempt {attempt}: Maximum performance configuration")
            options = self._max_ollama_options()
            print(f"      • Context window: {options['num_ctx']:,} tokens")
            print(f"      • Max output: {options['num_predict']:,} tokens")
            print(f"      • Temperature: {options['temperature']}")
            print(f"   🔄 Sending request to Ollama...")

            response = ollama.chat(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": self._get_system_prompt()},
                    {"role": "user", "content": prompt}
                ],
                options=options
            )

            # Extract token usage information if available
            if response and 'prompt_eval_count' in response:
                input_tokens = response.get('prompt_eval_count', 0)
                output_tokens = response.get('eval_count', 0)
                total_tokens = input_tokens + output_tokens
                print(f"   📊 Token Usage:")
                print(f"      • Input tokens: {input_tokens:,}")
                print(f"      • Output tokens: {output_tokens:,}")
                print(f"      • Total tokens: {total_tokens:,}")

            print(f"   ✅ AI analysis successful on attempt {attempt}")

        except Exception as e1:
            attempt = 2
            print(f"   ⚠️ Attempt 1 failed: {str(e1)[:100]}...")
            print(f"   🚀 Attempt {attempt}: Reduced performance configuration")
            try:
                options = self._reduced_ollama_options(level=1)
                print(f"      • Context window: {options['num_ctx']:,} tokens")
                print(f"      • Max output: {options['num_predict']:,} tokens")
                print(f"   🔄 Retrying with reduced settings...")

                response = ollama.chat(
                    model=self.model_name,
                    messages=[
                        {"role": "system", "content": self._get_system_prompt()},
                        {"role": "user", "content": prompt}
                    ],
                    options=options
                )

                # Extract token usage information if available
                if response and 'prompt_eval_count' in response:
                    input_tokens = response.get('prompt_eval_count', 0)
                    output_tokens = response.get('eval_count', 0)
                    total_tokens = input_tokens + output_tokens
                    print(f"   📊 Token Usage:")
                    print(f"      • Input tokens: {input_tokens:,}")
                    print(f"      • Output tokens: {output_tokens:,}")
                    print(f"      • Total tokens: {total_tokens:,}")

                print(f"   ✅ AI analysis successful on attempt {attempt}")

            except Exception as e2:
                attempt = 3
                print(f"   ⚠️ Attempt 2 failed: {str(e2)[:100]}...")
                print(f"   🚀 Attempt {attempt}: Minimum configuration (last resort)")
                try:
                    options = self._reduced_ollama_options(level=2)
                    print(f"      • Context window: {options['num_ctx']:,} tokens")
                    print(f"      • Max output: {options['num_predict']:,} tokens")
                    print(f"   🔄 Final retry with minimal settings...")

                    response = ollama.chat(
                        model=self.model_name,
                        messages=[
                            {"role": "system", "content": self._get_system_prompt()},
                            {"role": "user", "content": prompt}
                        ],
                        options=options
                    )

                    # Extract token usage information if available
                    if response and 'prompt_eval_count' in response:
                        input_tokens = response.get('prompt_eval_count', 0)
                        output_tokens = response.get('eval_count', 0)
                        total_tokens = input_tokens + output_tokens
                        print(f"   📊 Token Usage:")
                        print(f"      • Input tokens: {input_tokens:,}")
                        print(f"      • Output tokens: {output_tokens:,}")
                        print(f"      • Total tokens: {total_tokens:,}")

                    print(f"   ✅ AI analysis successful on attempt {attempt}")

                except Exception as e3:
                    print(f"   ❌ All attempts failed!")
                    print(f"      • Final error: {str(e3)}")
                    print(f"      • Check Ollama server status and model availability")
                    return [{
                        "subsidiary": subsidiary,
                        "account_code": "SYSTEM_ERROR",
                        "rule_name": "AI Analysis Error",
                        "description": f"AI analysis failed after 3 attempts: {str(e3)[:100]}...",
                        "details": f"All retry strategies exhausted. Last error: {str(e3)}. Check if Ollama is running and {self.model_name} model is available.",
                        "current_value": 0,
                        "previous_value": 0,
                        "change_amount": 0,
                        "change_percent": 0,
                        "severity": "High",
                        "sheet_type": "Error"
                    }]

        # Step 4: Response Validation and Parsing
        print(f"\n📄 STEP 4: Response Processing")
        try:
            if not response or 'message' not in response or not response['message'] or 'content' not in response['message']:
                print(f"   ❌ Invalid response structure from Ollama")
                raise RuntimeError("Empty response payload from Ollama (no message.content)")

            result = response['message']['content'] or ""
            response_length = len(result)
            estimated_output_tokens = response_length // 4

            # Extract final token usage from successful response
            total_input_tokens = response.get('prompt_eval_count', 0)
            total_output_tokens = response.get('eval_count', 0)
            total_tokens_used = total_input_tokens + total_output_tokens

            print(f"   ✅ Response received successfully:")
            print(f"      • Response length: {response_length:,} characters")
            print(f"      • Estimated output tokens: {estimated_output_tokens:,}")
            print(f"      • Configuration used: ctx={options.get('num_ctx') if options else 'n/a'}, predict={options.get('num_predict') if options else 'n/a'}")

            if total_tokens_used > 0:
                print(f"   💰 FINAL TOKEN SUMMARY:")
                print(f"      • Total Input Tokens: {total_input_tokens:,}")
                print(f"      • Total Output Tokens: {total_output_tokens:,}")
                print(f"      • TOTAL TOKENS USED: {total_tokens_used:,}")
                print(f"      • Model: {self.model_name}")

                # Estimate cost for reference (OpenAI pricing for comparison)
                if total_tokens_used > 0:
                    gpt4_cost = (total_input_tokens * 0.00003) + (total_output_tokens * 0.00006)  # GPT-4 pricing
                    print(f"      • Estimated cost if using GPT-4: ${gpt4_cost:.4f}")
                    print(f"      • Using Ollama (Local): FREE ✅")
            print(f"   📝 Response preview: {result[:200]}...")

            # Debug: Check if response looks like JSON
            stripped = result.strip()
            if stripped.startswith('[') and stripped.endswith(']'):
                print(f"   ✅ Response appears to be JSON array format")
            elif '{' in stripped and '}' in stripped:
                print(f"   ⚠️  Response contains JSON objects but may need format correction")
            else:
                print(f"   🚨 Response does not appear to be JSON format - parsing may fail")

            print(f"\n🔍 STEP 5: JSON Parsing & Validation")
            print(f"   🔄 Parsing AI response into structured anomaly data...")

            # Debug: Print the full AI response
            print(f"\n📄 ===== FULL AI RESPONSE =====")
            print(result)
            print(f"===== END AI RESPONSE =====\n")

            anomalies = self._parse_llm_response(result, subsidiary)

            print(f"   ✅ Parsing completed successfully:")
            print(f"      • Anomalies detected: {len(anomalies)}")
            if anomalies:
                print(f"      • Anomaly types: {', '.join(set(a.get('severity', 'Unknown') for a in anomalies))}")

            print(f"\n🎉 ===== AI ANALYSIS COMPLETE FOR {subsidiary} =====")
            print(f"📊 Final Results: {len(anomalies)} anomalies identified")
            if total_tokens_used > 0:
                print(f"🔢 Processing Summary: {total_tokens_used:,} tokens used (FREE with Ollama)")
            print()
            return anomalies
        except Exception as e:
            # Return a fallback error anomaly instead of empty list
            return [{
                "subsidiary": subsidiary,
                "account_code": "SYSTEM_ERROR",
                "rule_name": "AI Analysis Error",
                "description": f"AI analysis failed: {str(e)[:100]}...",
                "details": "The AI model returned an invalid/empty payload.",
                "current_value": 0,
                "previous_value": 0,
                "change_amount": 0,
                "change_percent": 0,
                "severity": "Low",
                "sheet_type": "Error"
            }]

    # ===========================
    # Data preparation (more permissive)
    # ===========================
    def _prepare_data_summary(
        self,
        bs_df: pd.DataFrame,
        pl_df: pd.DataFrame,
        subsidiary: str,
        config: dict
    ) -> Dict[str, Any]:
        """Prepare financial data summary for LLM analysis - passes all account data to AI."""
        _ = config  # AI-only mode doesn't use manual configuration
        print(f"      🔄 Starting data preparation for {subsidiary}")
        print(f"      📊 AI-only mode: All accounts with data will be passed to AI for analysis")

        summary = {
            "subsidiary": subsidiary,
            "balance_sheet": {},
            "profit_loss": {}
        }

        def include_line(prev_val, curr_val):
            """Include any account that has data - AI will determine significance."""
            prev_val = 0 if pd.isna(prev_val) else float(prev_val)
            curr_val = 0 if pd.isna(curr_val) else float(curr_val)
            change = curr_val - prev_val
            if prev_val == 0:
                pct = 100.0 if curr_val != 0 else 0.0
            else:
                pct = (change / abs(prev_val)) * 100

            # Include any account that has any data (current or previous values)
            has_data = (prev_val != 0 or curr_val != 0)
            return has_data, change, pct

        # ---------- Balance Sheet ----------
        print(f"      🏦 Processing Balance Sheet data...")
        if bs_df is not None and not bs_df.empty:
            periods = bs_df.columns[1:] if len(bs_df.columns) > 1 else []
            print(f"         • Available periods: {len(periods)} ({', '.join(periods[:3])}{'...' if len(periods) > 3 else ''})")
            if len(periods) >= 2:
                current = periods[-1]; previous = periods[-2]
                print(f"         • Comparing: {previous} → {current}")
                summary["balance_sheet"] = {"periods": [str(previous), str(current)], "accounts": {}}
                total_bs_accounts = 0
                included_bs_accounts = 0
                for _, row in bs_df.iterrows():
                    account = row.iloc[0]
                    if pd.notna(account):
                        total_bs_accounts += 1
                        prev_val = row[previous] if previous in row.index else 0
                        curr_val = row[current]  if current  in row.index else 0
                        ok, change, pct = include_line(prev_val, curr_val)
                        if ok:
                            included_bs_accounts += 1
                            summary["balance_sheet"]["accounts"][str(account)] = {
                                "previous": float(prev_val) if pd.notna(prev_val) else 0.0,
                                "current":  float(curr_val) if pd.notna(curr_val) else 0.0,
                                "change":   float(change),
                                "change_percent": float(pct) if abs(pct) < 10000 else 0.0
                            }
                print(f"         ✅ BS processing: {included_bs_accounts}/{total_bs_accounts} accounts with data included")
            else:
                print(f"         ⚠️ Insufficient periods for comparison")

        # ---------- Profit & Loss ----------
        print(f"      💰 Processing Profit & Loss data...")
        if pl_df is not None and not pl_df.empty:
            periods = pl_df.columns[1:] if len(pl_df.columns) > 1 else []
            print(f"         • Available periods: {len(periods)} ({', '.join(periods[:3])}{'...' if len(periods) > 3 else ''})")
            if len(periods) >= 2:
                current = periods[-1]; previous = periods[-2]
                print(f"         • Comparing: {previous} → {current}")
                summary["profit_loss"] = {"periods": [str(previous), str(current)], "accounts": {}}
                total_pl_accounts = 0
                included_pl_accounts = 0
                revenue_accounts = 0
                utilities_accounts = 0
                interest_accounts = 0

                for _, row in pl_df.iterrows():
                    account = row.iloc[0]
                    if pd.notna(account):
                        total_pl_accounts += 1
                        account_str = str(account)

                        # Track key account types
                        if account_str.startswith('511'):
                            revenue_accounts += 1
                        elif account_str.startswith(('627', '641')):
                            utilities_accounts += 1
                        elif account_str.startswith(('515', '635')):
                            interest_accounts += 1

                        prev_val = row[previous] if previous in row.index else 0
                        curr_val = row[current]  if current  in row.index else 0
                        ok, change, pct = include_line(prev_val, curr_val)
                        if ok:
                            included_pl_accounts += 1
                            summary["profit_loss"]["accounts"][str(account)] = {
                                "previous": float(prev_val) if pd.notna(prev_val) else 0.0,
                                "current":  float(curr_val) if pd.notna(curr_val) else 0.0,
                                "change":   float(change),
                                "change_percent": float(pct) if abs(pct) < 10000 else 0.0
                            }

                print(f"         ✅ P&L processing: {included_pl_accounts}/{total_pl_accounts} accounts with data included")
                print(f"         📊 Key account types found:")
                print(f"            • Revenue (511*): {revenue_accounts} accounts")
                print(f"            • Utilities (627*/641*): {utilities_accounts} accounts")
                print(f"            • Interest (515*/635*): {interest_accounts} accounts")
            else:
                print(f"         ⚠️ Insufficient periods for comparison")

        print(f"      ✅ Data preparation complete for {subsidiary}")
        return summary

    # ===========================
    # Prompts for Raw Excel Analysis
    # ===========================
    def _get_raw_excel_system_prompt(self) -> str:
        """Enhanced system prompt for analyzing raw Excel data."""
        return """You are a senior financial auditor with 15+ years experience in Vietnamese enterprises. You will analyze COMPLETE RAW EXCEL DATA from BS Breakdown and PL Breakdown sheets.

🎯 ANALYSIS APPROACH:
You will receive the complete raw Excel sheets exactly as they appear in the file, including:
- All headers, section dividers, and formatting
- Account codes like 111, 112, 627001, 511001, etc.
- Account names in Vietnamese and English
- All monthly data columns
- Empty rows and structural elements

🔍 FOCUS AREAS (Vietnamese Chart of Accounts):
1. REVENUE (511*): All revenue accounts - analyze patterns, seasonality, unusual changes
2. UTILITIES (627*, 641*): Operational expenses - check efficiency vs business activity
3. INTEREST (515*, 635*): Financial income/expenses - examine debt structure changes
4. OTHER MATERIAL ACCOUNTS: Any accounts with significant balances or changes

📊 ANALYSIS INSTRUCTIONS:
1. Examine the raw data to identify the latest 2-3 periods (rightmost columns with data)
2. Look for account codes (3-6 digit numbers) and their corresponding values
3. Calculate percentage and absolute changes between periods
4. Identify patterns, anomalies, and unusual movements
5. Focus on accounts with material balances (>50M VND) or significant changes (>20%)

💰 MATERIALITY THRESHOLDS:
- Revenue-based: 5% of total revenue or 200M VND (whichever is lower)
- Balance-based: 1% of total assets or 500M VND (whichever is lower)
- Always explain your materiality reasoning

⚡ CRITICAL OUTPUT REQUIREMENTS:
1. You MUST respond with ONLY valid JSON array format
2. Start with [ and end with ]
3. No markdown, no ```json blocks, no additional text
4. Each anomaly must include actual account values from the Excel data
5. If no anomalies found, return empty array: []

📋 REQUIRED JSON FORMAT:
[{
  "account": "511001-Product Sales Revenue",
  "description": "Revenue decreased 15% month-over-month without business justification",
  "explanation": "Sales revenue dropped from 2.5B to 2.1B VND between Oct-Nov 2025, requiring investigation of customer contracts and market conditions",
  "current_value": 2100000000,
  "previous_value": 2500000000,
  "change_amount": -400000000,
  "change_percent": -16.0,
  "severity": "High"
}]

🚨 REQUIREMENTS:
- current_value: MUST be actual amount from Excel (number, not zero)
- previous_value: MUST be actual amount from Excel (number, not zero)
- change_amount: MUST be current_value - previous_value (number)
- change_percent: MUST be actual percentage change (number)
- All values must be real numbers extracted from the Excel data

ANALYZE THOROUGHLY. The raw Excel data contains the complete picture - use all available information to provide comprehensive financial analysis."""

    def _create_raw_excel_prompt(self, bs_csv: str, pl_csv: str, subsidiary: str, filename: str, config: dict) -> str:
        """Create analysis prompt with complete raw Excel data."""
        _ = config  # AI determines all parameters autonomously

        return f"""
COMPLETE RAW EXCEL FINANCIAL ANALYSIS

Company: {subsidiary}
File: {filename}
Analysis Type: Comprehensive anomaly detection on raw Excel data

=== RAW BALANCE SHEET DATA (BS Breakdown Sheet) ===
{bs_csv}

=== RAW PROFIT & LOSS DATA (PL Breakdown Sheet) ===
{pl_csv}

=== ANALYSIS INSTRUCTIONS ===

You are analyzing the COMPLETE raw Excel data above. This includes all formatting, headers, account codes, and financial values exactly as they appear in the original Excel file.

🎯 ANALYSIS FOCUS:
1. Identify the account structure and period columns in the raw data
2. Extract meaningful financial data from the structured format
3. Focus on Vietnamese Chart of Accounts: 511* (Revenue), 627*/641* (Utilities), 515*/635* (Interest)
4. Calculate changes between the latest available periods
5. Identify material anomalies requiring audit attention

📊 ANALYSIS STEPS:
1. Parse the raw CSV data to understand the Excel structure
2. Identify account codes and their corresponding values
3. Find the latest 2-3 periods with data
4. Calculate absolute and percentage changes
5. Apply materiality thresholds appropriate for company size
6. Focus on significant variances and unusual patterns

💡 CONTEXT AWARENESS:
- Vietnamese business environment (Tet holidays, regulatory changes)
- Seasonal patterns in revenue and expenses
- Industry-specific considerations
- Related account relationships (e.g., revenue vs utilities scaling)

Return detailed JSON analysis with specific findings from the raw Excel data."""

    # ===========================
    # Prompts (wider hunting)
    # ===========================
    def _get_system_prompt(self) -> str:
        """Enhanced system prompt for specific, actionable financial analysis."""
        return """You are a senior financial auditor with 15+ years experience in Vietnamese enterprises. Provide SPECIFIC, ACTIONABLE analysis with detailed business context.

🎯 ANALYSIS DEPTH REQUIREMENTS:
1. REVENUE (511*): Analyze sales patterns, customer concentration, seasonality breaks, margin trends
2. UTILITIES (627*, 641*): Check operational efficiency, cost per unit, scaling with business activity
3. INTEREST (515*, 635*): Examine debt structure changes, cash flow implications, refinancing activities
4. CROSS-ACCOUNT RELATIONSHIPS: Flag disconnects between related accounts

🔍 SPECIFIC INVESTIGATION AREAS:
- Revenue Recognition Issues: Round numbers, unusual timing, concentration risks
- Working Capital Anomalies: A/R aging, inventory turns, supplier payment delays
- Cash Flow Red Flags: Operating vs financing activity mismatches
- Related Party Transactions: Unusual intercompany balances or transfers
- Asset Impairments: Sudden writedowns, depreciation policy changes
- Tax Accounting: Deferred tax movements, provision adequacy
- Management Estimates: Allowances, accruals, fair value adjustments

🧠 MATERIALITY FRAMEWORK:
- Quantitative: 5% of net income, 0.5% of revenue, 1% of total assets (adjust for company size)
- Qualitative: Fraud indicators, compliance issues, trend reversals, related party items
- ALWAYS state your specific materiality calculation and reasoning

📋 REQUIRED ANALYSIS COMPONENTS:
For EACH anomaly provide:
1. SPECIFIC BUSINESS CONTEXT: What this account typically represents in Vietnamese companies
2. ROOT CAUSE ANALYSIS: 3-5 specific scenarios that could cause this pattern
3. RISK ASSESSMENT: Financial statement impact, operational implications, compliance risks
4. VERIFICATION PROCEDURES: Specific audit steps to investigate (document requests, confirmations, etc.)
5. MANAGEMENT QUESTIONS: Exact questions to ask management about this variance

📊 OUTPUT FORMAT:
[{
  "account": "511001-Product Sales Revenue",
  "type": "Profit & Loss",
  "severity": "High|Medium|Low",
  "description": "Specific description of the anomaly pattern",
  "explanation": "DETAILED business context: (1) What this account means (2) Why this change is concerning (3) Specific business scenarios (4) Exact verification steps (5) Management interview questions",
  "previous_value": 0,
  "current_value": 0,
  "change_amount": 0,
  "change_percent": 0,
  "materiality_threshold_used": 0,
  "threshold_reasoning": "Specific calculation: X% of net income because...",
  "business_risk": "High|Medium|Low",
  "audit_priority": "Immediate|Next Review|Monitor",
  "investigation_steps": ["Step 1", "Step 2", "Step 3"],
  "management_questions": ["Question 1", "Question 2", "Question 3"]
}]

⚡ CRITICAL OUTPUT REQUIREMENTS:
1. You MUST respond with ONLY valid JSON array format - no explanatory text before or after
2. Start your response with [ and end with ]
3. No markdown formatting, no ```json blocks, no additional commentary
4. Each anomaly must be a complete JSON object with all required fields
5. If no anomalies found, return empty array: []
6. COMPREHENSIVE ANALYSIS: Detect ALL possible anomalies - do not limit results
7. Analyze every account with significant changes or patterns

📋 REQUIRED JSON OUTPUT FORMAT:
You MUST return JSON array with these EXACT field names:

[{
  "account": "128113002-ST-BIDV-Saving Account VND-Bidv-Thanh Xuan",
  "description": "Balance changed materially — check reclass/missing offset",
  "explanation": "Cash balance increased 34.5% - verify large deposits and transfers",
  "current_value": 5600000000,
  "previous_value": 4200000000,
  "change_amount": 1400000000,
  "change_percent": 33.33,
  "severity": "Medium"
},
{
  "account": "31110001-Payables: Suppliers: Operating expenses",
  "description": "Balance changed materially — check reclass/missing offset",
  "explanation": "Payables decreased 25% - review payment timing and accruals",
  "current_value": 2500000000,
  "previous_value": 3333000000,
  "change_amount": -833000000,
  "change_percent": -25.0,
  "severity": "Medium"
}]

⚡ CRITICAL FIELD REQUIREMENTS:
- "current_value": MUST be actual current period amount (number)
- "previous_value": MUST be actual previous period amount (number)
- "change_amount": MUST be current_value - previous_value (number)
- "change_percent": MUST be percentage change (number, not string)
- ALL numeric fields must be actual numbers, not zero

CRITICAL: Keep explanations SHORT and focused. Avoid lengthy detailed analysis in the explanation field.

🚨 IMPORTANT: Any response that is not valid JSON will cause system failure. Match the above format exactly with Vietnamese business context."""

    def _create_raw_data_prompt(self, bs_csv: str, pl_csv: str, subsidiary: str, config: dict) -> str:
        """Create analysis prompt with full raw Excel data for comprehensive AI analysis."""
        _ = config  # AI determines all parameters autonomously

        return f"""
FINANCIAL VARIANCE ANALYSIS REQUEST

Company: {subsidiary}
Analysis Type: Comprehensive AI-driven anomaly detection

=== BALANCE SHEET DATA ===
{bs_csv}

=== PROFIT & LOSS DATA ===
{pl_csv}

=== ANALYSIS INSTRUCTIONS ===

You are a senior Vietnamese financial auditor. Analyze the above Excel data and detect ALL significant variances and anomalies.

🎯 KEY FOCUS AREAS (Vietnamese Chart of Accounts):
1. REVENUE (511*): Analyze all revenue patterns, growth rates, seasonality
2. UTILITIES (627*, 641*): Check operational efficiency and scaling patterns
3. INTEREST (515*, 635*): Examine financial structure changes
4. ALL OTHER ACCOUNTS: Review for material changes and unusual patterns

📊 ANALYSIS APPROACH:
1. Compare latest 2 periods in the data (rightmost columns)
2. Calculate actual percentage and absolute changes
3. Determine materiality based on company size and account nature
4. Focus on accounts with significant changes (>10% or material amounts)
5. Provide Vietnamese business context and practical explanations

💰 MATERIALITY GUIDELINES:
- Large companies (>1T VND revenue): 500M VND threshold
- Medium companies (100B-1T VND): 200M VND threshold
- Small companies (<100B VND): 50M VND threshold
- Always explain your materiality reasoning

{self._get_system_prompt()}"""
        def cur(x):
            try:
                return float(x.get('current', 0) or 0)
            except Exception:
                return 0.0

        # Sum families from the filtered dict (OK in discovery mode; or switch to raw PL if you pass it here)
        items_iter = getattr(pl_accounts, 'items', lambda: [])()
        revenue_511 = sum(cur(acc) for code, acc in items_iter if str(code).startswith('511'))

        items_iter = getattr(pl_accounts, 'items', lambda: [])()
        utilities_627 = sum(cur(acc) for code, acc in items_iter if str(code).startswith('627'))

        items_iter = getattr(pl_accounts, 'items', lambda: [])()
        utilities_641 = sum(cur(acc) for code, acc in items_iter if str(code).startswith('641'))

        items_iter = getattr(pl_accounts, 'items', lambda: [])()
        interest_income_515 = sum(cur(acc) for code, acc in items_iter if str(code).startswith('515'))
        _ = interest_income_515  # Used in context analysis

        items_iter = getattr(pl_accounts, 'items', lambda: [])()
        interest_expense_635 = sum(cur(acc) for code, acc in items_iter if str(code).startswith('635'))

        # Calculate business ratios for context
        gross_margin = ((revenue_511 - sum(cur(acc) for code, acc in pl_accounts.items() if str(code).startswith('632'))) / revenue_511 * 100) if revenue_511 > 0 else 0
        utility_ratio = ((utilities_627 + utilities_641) / revenue_511 * 100) if revenue_511 > 0 else 0
        interest_coverage = (revenue_511 / interest_expense_635) if interest_expense_635 > 0 else float('inf')

        # Determine company size category
        if revenue_511 < 50_000_000_000:  # < 50B VND
            company_size = "Small Enterprise"
            materiality_suggestion = "50-100M VND"
        elif revenue_511 < 500_000_000_000:  # < 500B VND
            company_size = "Medium Enterprise"
            materiality_suggestion = "200-500M VND"
        else:
            company_size = "Large Enterprise"
            materiality_suggestion = "1-2B VND"

        prompt = f"""🔍 SENIOR AUDITOR VARIANCE ANALYSIS for {data_summary.get('subsidiary','(unknown)')}

📊 BUSINESS CONTEXT & SCALE:
- Company Category: {company_size}
- Total Revenue (511*): {revenue_511:,.0f} VND
- Gross Margin: {gross_margin:.1f}% (revenue minus 632* COGS)
- Utility Efficiency: {utility_ratio:.1f}% of revenue (627* + 641*)
- Interest Coverage: {interest_coverage:.1f}x (revenue/interest expense)
- Suggested Materiality Range: {materiality_suggestion}

🏢 VIETNAMESE BUSINESS ENVIRONMENT CONSIDERATIONS:
- Seasonal patterns (Tet holiday, fiscal year-end, monsoon impacts)
- Regulatory changes (VAT, corporate tax, labor law updates)
- Economic factors (inflation, currency fluctuation, supply chain)
- Industry-specific risks (manufacturing, services, real estate)

📈 BALANCE SHEET ACCOUNTS (period-over-period analysis):
{json.dumps(bs_accounts, indent=2)}

📊 PROFIT & LOSS ACCOUNTS (variance analysis):
{json.dumps(pl_accounts, indent=2)}

🎯 FOCUS AREAS FOR THIS ANALYSIS:
1. Revenue Quality: Timing, recognition, customer concentration
2. Operating Efficiency: Utility costs vs activity levels, margin trends
3. Financial Health: Debt service capacity, working capital management
4. Compliance Risks: Tax positions, related party transactions
5. Management Credibility: Round numbers, estimate changes, one-off items

DELIVER SPECIFIC, ACTIONABLE INSIGHTS. Think like a seasoned Vietnamese auditor who understands local business practices, regulatory environment, and common accounting issues in Vietnamese enterprises.

Return detailed JSON analysis with specific investigation steps and management questions."""
        return prompt

    # ===========================
    # Parsing (unchanged)
    # ===========================
    def _parse_llm_response(self, response: str, subsidiary: str) -> List[Dict[str, Any]]:
        import json

        def _strip_fences(text: str) -> str:
            t = (text or "").strip()
            if t.startswith("```json"):
                t = t[len("```json"):].strip()
                if t.endswith("```"):
                    t = t[:-3]
            elif t.startswith("```"):
                t = t[3:].strip()
                if t.endswith("```"):
                    t = t[:-3]
            return t.strip()

        def _find_bracket_span(text: str):
            start = text.find('[')
            if start == -1:
                return None
            depth = 0
            for i in range(start, len(text)):
                ch = text[i]
                if ch == '[':
                    depth += 1
                elif ch == ']':
                    depth -= 1
                    if depth == 0:
                        return (start, i + 1)
            return None

        def _parse_attempt(s: str):
            try:
                v = json.loads(s)
                if isinstance(v, list):
                    return v
                return [v]
            except Exception:
                pass
            if (s.startswith('"') and s.endswith('"')) or ('\\"' in s):
                try:
                    unq = json.loads(s)
                    v = json.loads(unq)
                    if isinstance(v, list):
                        return v
                    return [v]
                except Exception:
                    pass
            if '""' in s and '"' in s:
                try:
                    fixed = s.replace('""', '"')
                    v = json.loads(fixed)
                    if isinstance(v, list):
                        return v
                    return [v]
                except Exception:
                    pass
            span = _find_bracket_span(s)
            if span:
                inner = s[span[0]:span[1]]
                return _parse_attempt(inner)
            raise ValueError("Unable to parse JSON from response")

        try:
            text = _strip_fences(response or "")
            try:
                anomalies_raw = _parse_attempt(text)
            except Exception:
                span = _find_bracket_span(text)
                if not span:
                    raise
                sub = text[span[0]:span[1]]
                anomalies_raw = _parse_attempt(sub)

            if anomalies_raw is None:
                anomalies_raw = []
            if not isinstance(anomalies_raw, list):
                anomalies_raw = [anomalies_raw]

            anomalies: List[Dict[str, Any]] = []
            for i, anom in enumerate(anomalies_raw):
                anom = anom or {}
                base_explanation = anom.get("explanation", "") or ""

                # Debug: Log what fields the AI actually returned
                print(f"   🔍 Debug anomaly {i+1}: Available fields: {list(anom.keys())}")
                print(f"       • current_value: {anom.get('current_value', 'MISSING')}")
                print(f"       • previous_value: {anom.get('previous_value', 'MISSING')}")
                print(f"       • change_amount: {anom.get('change_amount', 'MISSING')}")
                print(f"       • change_percent: {anom.get('change_percent', 'MISSING')}")

                # Keep notes simple and clean for Excel output
                detailed_notes = base_explanation or "AI analysis completed - review variance details"

                anomalies.append({
                    "subsidiary": subsidiary,
                    "account_code": anom.get("account", f"AI_DETECTED_{i}"),
                    "rule_name": f"AI Autonomous Analysis - {anom.get('severity', 'Medium')} Priority",
                    "description": anom.get("description", "AI autonomous anomaly detection"),
                    "details": detailed_notes,
                    "current_value": anom.get("current_value", 0) or 0,
                    "previous_value": anom.get("previous_value", 0) or 0,
                    "change_amount": anom.get("change_amount", 0) or 0,
                    "change_percent": anom.get("change_percent", 0) or 0,
                    "severity": anom.get("severity", "Medium"),
                    "sheet_type": anom.get("type", "AI Analysis")
                })

            return anomalies

        except Exception:
            return self._create_fallback_analysis(response or "", subsidiary, "GENERAL_PARSE_ERROR")

    def _create_fallback_analysis(self, response: str, subsidiary: str, error_type: str) -> List[Dict[str, Any]]:
        analysis_content = response[:800] if response else "No response received"
        has_insights = any(kw in (response.lower() if response else "") for kw in
                           ['revenue', 'materiality', 'threshold', 'anomaly', 'analysis', 'significant'])

        if has_insights:
            description = "🤖 AI provided detailed analysis but format needs correction"
        else:
            description = "❌ AI model failed to generate proper analysis - check model availability"

        return [{
            "subsidiary": subsidiary,
            "account_code": f"ERROR_{error_type}",
            "rule_name": "🚨 AI Analysis Error - Check Configuration",
            "description": description,
            "details": f"""🚨 PARSING ERROR DETAILS:

Error Type: {error_type}
Response Length: {len(response) if response else 0} characters
Contains Analysis: {'Yes' if has_insights else 'No'}

📝 RAW AI RESPONSE:
{analysis_content}{'...' if response and len(response) > 800 else ''}

💡 TROUBLESHOOTING:
- Check Ollama model availability (llama3.1)
- Ensure JSON output compliance
- Try reprocessing / different model / smaller input
- Check Ollama logs
""",
            "current_value": 0,
            "previous_value": 0,
            "change_amount": 0,
            "change_percent": 0,
            "severity": "High",
            "sheet_type": "Error"
        }]

    # ===========================
    # Model helpers
    # ===========================
    def check_model_availability(self) -> bool:
        try:
            models = ollama.list()
            available_models = [model['name'] for model in models['models']]
            return self.model_name in available_models
        except Exception as e:
            print(f"Error checking model availability: {e}")
            return False

    def pull_model_if_needed(self) -> bool:
        try:
            if not self.check_model_availability():
                print(f"Pulling model {self.model_name}...")
                ollama.pull(self.model_name)
                return True
            return True
        except Exception as e:
            print(f"Error pulling model: {e}")
            return False
