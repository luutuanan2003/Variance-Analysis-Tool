import json
import io
import os
import sys
from typing import List, Dict, Any
import pandas as pd
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class LLMFinancialAnalyzer:
    def __init__(self, model_name: str = "gpt-4o"):
        """Initialize LLM analyzer with OpenAI GPT model."""
        # Debug information for cloud deployments
        print(f"🔧 Python version: {sys.version}")
        print(f"🔧 Environment: {'RENDER' if os.getenv('RENDER') else 'LOCAL'}")

        # Get OpenAI configuration from environment
        self.openai_model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.openai_api_key = os.getenv("OPENAI_API_KEY")

        if not self.openai_api_key or self.openai_api_key == "your_openai_api_key_here":
            raise ValueError(
                "OpenAI API key not found! Please set OPENAI_API_KEY in your .env file.\n"
                "Get your API key from: https://platform.openai.com/api-keys"
            )

        # Initialize OpenAI client with comprehensive error handling for deployment environments
        client_kwargs = {"api_key": self.openai_api_key}

        # Try multiple initialization approaches for different environments
        initialization_attempts = [
            lambda: OpenAI(**client_kwargs),
            lambda: OpenAI(api_key=self.openai_api_key),  # Explicit API key only
            lambda: self._init_openai_minimal(),  # Minimal initialization for cloud environments
            lambda: self._init_openai_aggressive(),  # Most aggressive approach for stubborn cases
        ]

        self.openai_client = None
        last_error = None

        for attempt_num, init_func in enumerate(initialization_attempts, 1):
            try:
                print(f"🔄 Attempting OpenAI client initialization (attempt {attempt_num})...")
                self.openai_client = init_func()
                print(f"✅ OpenAI client initialized successfully on attempt {attempt_num}")
                break
            except TypeError as e:
                last_error = e
                error_msg = str(e).lower()
                print(f"⚠️  Attempt {attempt_num} failed: {e}")

                if "proxies" in error_msg:
                    print("   → Issue related to proxy parameter - trying next approach")
                    continue
                elif "unexpected keyword argument" in error_msg:
                    print("   → Unexpected parameter issue - trying simpler initialization")
                    continue
                else:
                    print(f"   → Unknown TypeError: {e}")
                    continue
            except Exception as e:
                last_error = e
                print(f"⚠️  Attempt {attempt_num} failed with unexpected error: {e}")
                continue

        if self.openai_client is None:
            raise RuntimeError(f"Failed to initialize OpenAI client after {len(initialization_attempts)} attempts. Last error: {last_error}")
        print(f"🤖 Using OpenAI model: {self.openai_model}")
        print(f"🔑 API key configured: {self.openai_api_key[:8]}...{self.openai_api_key[-4:]}")

    def _init_openai_minimal(self):
        """Minimal OpenAI initialization for cloud environments that may have issues with advanced parameters."""
        # Clear any proxy-related environment variables that might interfere
        proxy_vars = ['HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy', 'ALL_PROXY', 'all_proxy']
        original_values = {}
        for var in proxy_vars:
            if var in os.environ:
                original_values[var] = os.environ[var]
                del os.environ[var]
                print(f"   → Temporarily cleared {var} environment variable")

        try:
            # Try monkey-patching the OpenAI Client to ignore proxies parameter
            import openai
            original_init = openai.OpenAI.__init__

            def patched_init(self, **kwargs):
                # Remove any proxy-related parameters that might cause issues
                clean_kwargs = {k: v for k, v in kwargs.items() if k not in ['proxies', 'proxy', 'http_client']}
                return original_init(self, **clean_kwargs)

            # Temporarily patch the __init__ method
            openai.OpenAI.__init__ = patched_init

            try:
                client = openai.OpenAI(api_key=self.openai_api_key)
                print("   → Successfully initialized with monkey patch")
            finally:
                # Restore original __init__ method
                openai.OpenAI.__init__ = original_init

            # Restore original environment variables
            for var, value in original_values.items():
                os.environ[var] = value

            return client
        except Exception as e:
            print(f"   → Minimal initialization also failed: {e}")
            # Restore environment variables even if failed
            for var, value in original_values.items():
                os.environ[var] = value
            raise e

    def _init_openai_aggressive(self):
        """Simplified aggressive OpenAI initialization for problematic cloud environments."""
        print("   → Attempting aggressive initialization with module isolation")

        # Clear ALL possible proxy and network-related environment variables
        network_vars = [
            'HTTP_PROXY', 'HTTPS_PROXY', 'http_proxy', 'https_proxy',
            'ALL_PROXY', 'all_proxy', 'NO_PROXY', 'no_proxy'
        ]
        original_values = {}
        for var in network_vars:
            if var in os.environ:
                original_values[var] = os.environ[var]
                del os.environ[var]
                print(f"   → Cleared {var}")

        try:
            # Create a new isolated process to initialize OpenAI
            import subprocess
            import json

            # Create a simple script to initialize OpenAI client
            test_script = f'''
import os
import json
from openai import OpenAI

try:
    client = OpenAI(api_key="{self.openai_api_key}")
    print(json.dumps({{"success": True, "message": "OpenAI client created successfully"}}))
except Exception as e:
    print(json.dumps({{"success": False, "error": str(e)}}))
'''

            # Run in isolated subprocess
            result = subprocess.run([sys.executable, '-c', test_script],
                                  capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                try:
                    output = json.loads(result.stdout.strip())
                    if output.get('success'):
                        # If subprocess succeeded, try here too with cleared environment
                        from openai import OpenAI
                        client = OpenAI(api_key=self.openai_api_key)
                        print("   → Successfully created OpenAI client after environment isolation")

                        # Restore environment variables
                        for var, value in original_values.items():
                            os.environ[var] = value

                        return client
                except Exception:
                    pass

            raise RuntimeError("Subprocess initialization test failed")

        except Exception as e:
            print(f"   → Aggressive initialization failed: {e}")
            # Restore environment variables
            for var, value in original_values.items():
                os.environ[var] = value
            raise e

    # ===========================
    # OpenAI API Methods
    # ===========================
    def _call_openai(self, system_prompt: str, user_prompt: str) -> dict:
        """Call OpenAI API."""
        try:
            print(f"   🔄 Making OpenAI API call...")
            print(f"      • Model: {self.openai_model}")
            print(f"      • System prompt length: {len(system_prompt)} chars")
            print(f"      • User prompt length: {len(user_prompt)} chars")

            response = self.openai_client.chat.completions.create(
                model=self.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.1,
                max_tokens=4000
            )

            print(f"   ✅ OpenAI API call completed")
            print(f"      • Response type: {type(response)}")

            # Validate response structure
            if not response:
                raise RuntimeError("OpenAI API returned empty response")

            print(f"      • Has choices: {hasattr(response, 'choices')}")
            if not response.choices or len(response.choices) == 0:
                raise RuntimeError("OpenAI API returned no choices")

            print(f"      • Choices count: {len(response.choices)}")
            if not response.choices[0].message:
                raise RuntimeError("OpenAI API returned no message")

            print(f"      • Has message: {hasattr(response.choices[0], 'message')}")
            content = response.choices[0].message.content
            print(f"      • Content type: {type(content)}")
            print(f"      • Content length: {len(content) if content else 0}")

            if content is None:
                raise RuntimeError("OpenAI API returned None content")

            # Return in consistent format
            return {
                "message": {
                    "content": content
                },
                "prompt_eval_count": response.usage.prompt_tokens if response.usage else 0,
                "eval_count": response.usage.completion_tokens if response.usage else 0,
                "total_tokens": response.usage.total_tokens if response.usage else 0
            }
        except Exception as e:
            print(f"   ❌ OpenAI API call failed: {str(e)}")
            print(f"      • Error type: {type(e)}")
            import traceback
            print(f"      • Traceback: {traceback.format_exc()}")
            raise RuntimeError(f"OpenAI API call failed: {str(e)}")


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
        print(f"🤖 Model: {self.openai_model}")

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

            # Convert raw DataFrames to CSV - keep all rows but optimize format
            # Remove completely empty rows and columns to reduce token usage
            bs_clean = bs_raw.dropna(how='all').dropna(axis=1, how='all')
            pl_clean = pl_raw.dropna(how='all').dropna(axis=1, how='all')

            # Use more compact CSV format but INCLUDE headers so AI can see period names
            bs_csv = bs_clean.to_csv(index=False, header=True, quoting=1, float_format='%.0f')
            pl_csv = pl_clean.to_csv(index=False, header=True, quoting=1, float_format='%.0f')

            print(f"   ✅ CSV conversion complete (optimized format):")
            print(f"      • BS CSV: {len(bs_csv):,} characters (from {len(bs_raw)} rows to {len(bs_clean)} rows)")
            print(f"      • PL CSV: {len(pl_csv):,} characters (from {len(pl_raw)} rows to {len(pl_clean)} rows)")

            # Debug: Show sample of CSV data
            print(f"   🔍 Debug: BS CSV sample (first 500 chars):")
            print(f"      {bs_csv[:500]}...")
            print(f"   🔍 Debug: PL CSV sample (first 500 chars):")
            print(f"      {pl_csv[:500]}...")

            print(f"\n📝 STEP 3: Creating AI Analysis Prompt")

            # Check if data will exceed token limits and chunk if necessary
            estimated_prompt_length = len(bs_csv) + len(pl_csv) + 10000  # Add system prompt overhead
            estimated_tokens = estimated_prompt_length // 4

            print(f"   📊 Token estimation:")
            print(f"      • Estimated prompt length: {estimated_prompt_length:,} characters")
            print(f"      • Estimated input tokens: {estimated_tokens:,}")

            if estimated_tokens > 25000:  # Leave buffer for 30k limit
                print(f"   ⚠️  Data too large, implementing chunking strategy...")
                return self._analyze_with_chunking(bs_clean, pl_clean, subsidiary, filename, config)

            prompt = self._create_raw_excel_prompt(bs_csv, pl_csv, subsidiary, filename, config)
            prompt_length = len(prompt)
            print(f"   ✅ Prompt generation complete:")
            print(f"      • Total prompt length: {prompt_length:,} characters")

            print(f"\n🤖 STEP 4: AI Model Processing")
            response = None
            options = None
            attempt = 1

            try:
                print(f"   🚀 Attempt {attempt}: OpenAI GPT-4o processing")
                print(f"   🔄 Sending complete raw Excel data to AI...")

                response = self._call_openai(
                    system_prompt=self._get_raw_excel_system_prompt(),
                    user_prompt=prompt
                )

                # Extract token usage information if available
                if response and 'total_tokens' in response:
                    input_tokens = response.get('total_tokens', 0)
                    output_tokens = response.get('eval_count', 0)
                    total_tokens = response.get('total_tokens', 0)
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
            print(f"   🔍 Debug: Response type: {type(response)}")
            print(f"   🔍 Debug: Response keys: {list(response.keys()) if response else 'None'}")

            if not response:
                print(f"   ❌ Response is None or empty")
                raise RuntimeError("OpenAI API returned None response")

            if 'message' not in response:
                print(f"   ❌ No 'message' key in response")
                raise RuntimeError("OpenAI API response missing 'message' key")

            if not response['message']:
                print(f"   ❌ Response message is None")
                raise RuntimeError("OpenAI API response message is None")

            if 'content' not in response['message']:
                print(f"   ❌ No 'content' key in message")
                raise RuntimeError("OpenAI API response missing 'content' key")

            if response['message']['content'] is None:
                print(f"   ❌ Response content is None")
                raise RuntimeError("OpenAI API returned None content")

            result = response['message']['content'] or ""
            response_length = len(result)

            # Extract final token usage from successful response
            total_input_tokens = response.get('total_tokens', 0)
            total_output_tokens = response.get('eval_count', 0)
            total_tokens_used = total_input_tokens + total_output_tokens

            print(f"   ✅ Response received successfully:")
            print(f"      • Response length: {response_length:,} characters")
            if total_tokens_used > 0:
                print(f"   💰 FINAL TOKEN SUMMARY:")
                print(f"      • Total Input Tokens: {total_input_tokens:,}")
                print(f"      • Total Output Tokens: {total_output_tokens:,}")
                print(f"      • TOTAL TOKENS USED: {total_tokens_used:,}")
                print(f"      • Model: {self.openai_model}")

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
                print(f"🔢 Processing Summary: {total_tokens_used:,} tokens used (FREE with OpenAI)")
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
        print(f"   • Model: {self.openai_model}")

        # Quick sanity checks (both sheets should be non-empty by the time we get here)
        if pl_df is None or pl_df.empty:
            print("❌ ERROR: Profit & Loss data is empty or None")
            raise ValueError("Profit & Loss data is empty or None")
        if bs_df is None or bs_df.empty:
            print("❌ ERROR: Balance Sheet data is empty or None")
            raise ValueError("Balance Sheet data is empty or None")

        """
        Analyze financial data using OpenAI ChatGPT API to detect anomalies and provide explanations.
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
            print(f"   🚀 Attempt {attempt}: OpenAI GPT processing")
            print(f"   🔄 Sending request to OpenAI...")

            response = self._call_openai(
                system_prompt=self._get_system_prompt(),
                user_prompt=prompt
            )

            # Extract token usage information if available
            if response and 'total_tokens' in response:
                input_tokens = response.get('total_tokens', 0)
                output_tokens = response.get('eval_count', 0)
                total_tokens = response.get('total_tokens', 0)
                print(f"   📊 Token Usage:")
                print(f"      • Input tokens: {input_tokens:,}")
                print(f"      • Output tokens: {output_tokens:,}")
                print(f"      • Total tokens: {total_tokens:,}")

            print(f"   ✅ AI analysis successful on attempt {attempt}")

        except Exception as e1:
            attempt = 2
            print(f"   ⚠️ Attempt 1 failed: {str(e1)[:100]}...")
            print(f"   🚀 Attempt {attempt}: Retry with OpenAI GPT-4o")
            try:
                print(f"   🔄 Retrying with OpenAI API...")

                response = self._call_openai(
                    system_prompt=self._get_raw_excel_system_prompt(),
                    user_prompt=prompt
                )

                # Extract token usage information if available
                if response and 'total_tokens' in response:
                    input_tokens = response.get('total_tokens', 0)
                    output_tokens = response.get('eval_count', 0)
                    total_tokens = response.get('total_tokens', 0)
                    print(f"   📊 Token Usage:")
                    print(f"      • Input tokens: {input_tokens:,}")
                    print(f"      • Output tokens: {output_tokens:,}")
                    print(f"      • Total tokens: {total_tokens:,}")

                print(f"   ✅ AI analysis successful on attempt {attempt}")

            except Exception as e2:
                attempt = 3
                print(f"   ⚠️ Attempt 2 failed: {str(e2)[:100]}...")
                print(f"   🚀 Attempt {attempt}: Final retry with OpenAI GPT-4o")
                try:
                    print(f"   🔄 Final retry with OpenAI API...")

                    response = self._call_openai(
                        system_prompt=self._get_raw_excel_system_prompt(),
                        user_prompt=prompt
                    )

                    # Extract token usage information if available
                    if response and 'total_tokens' in response:
                        input_tokens = response.get('total_tokens', 0)
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
                    print(f"      • Check OpenAI server status and model availability")
                    return [{
                        "subsidiary": subsidiary,
                        "account_code": "SYSTEM_ERROR",
                        "rule_name": "AI Analysis Error",
                        "description": f"AI analysis failed after 3 attempts: {str(e3)[:100]}...",
                        "details": f"All retry strategies exhausted. Last error: {str(e3)}. Check if OpenAI is running and {self.openai_model} model is available.",
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
                print(f"   ❌ Invalid response structure from OpenAI")
                raise RuntimeError("Empty response payload from OpenAI (no message.content)")

            result = response['message']['content'] or ""
            response_length = len(result)
            estimated_output_tokens = response_length // 4

            # Extract final token usage from successful response
            total_input_tokens = response.get('total_tokens', 0)
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
                print(f"      • Model: {self.openai_model}")

                # Estimate cost for reference (OpenAI pricing for comparison)
                if total_tokens_used > 0:
                    gpt4_cost = (total_input_tokens * 0.00003) + (total_output_tokens * 0.00006)  # GPT-4 pricing
                    print(f"      • Estimated cost if using GPT-4: ${gpt4_cost:.4f}")
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
                print(f"🔢 Processing Summary: {total_tokens_used:,} tokens used (FREE with OpenAI)")
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

    def _analyze_with_chunking(self, bs_df, pl_df, subsidiary, filename, config):
        """Analyze large datasets by processing in chunks."""
        print(f"   🔄 Starting chunked analysis...")

        all_anomalies = []

        # Split BS data into chunks
        bs_chunk_size = 150
        pl_chunk_size = 75

        for i in range(0, len(bs_df), bs_chunk_size):
            bs_chunk = bs_df.iloc[i:i+bs_chunk_size]

            # For each BS chunk, process with a smaller PL chunk
            for j in range(0, len(pl_df), pl_chunk_size):
                pl_chunk = pl_df.iloc[j:j+pl_chunk_size]

                print(f"   📊 Processing chunk BS[{i}:{i+len(bs_chunk)}] + PL[{j}:{j+len(pl_chunk)}]")

                bs_csv = bs_chunk.to_csv(index=False, header=True, quoting=1, float_format='%.0f')
                pl_csv = pl_chunk.to_csv(index=False, header=True, quoting=1, float_format='%.0f')

                prompt = self._create_raw_excel_prompt(bs_csv, pl_csv, subsidiary, filename, config)

                try:
                    response = self._call_openai(
                        system_prompt=self._get_raw_excel_system_prompt(),
                        user_prompt=prompt
                    )

                    if response and response.get('message', {}).get('content'):
                        chunk_anomalies = self._parse_llm_response(response['message']['content'], subsidiary)
                        all_anomalies.extend(chunk_anomalies)
                        print(f"      ✅ Found {len(chunk_anomalies)} anomalies in this chunk")

                except Exception as e:
                    print(f"      ⚠️  Chunk failed: {str(e)[:100]}...")
                    continue

        print(f"   ✅ Chunked analysis complete: {len(all_anomalies)} total anomalies")
        return all_anomalies

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
- Account codes in various formats:
  * Simple codes: (111), (112), (120), (121) - Balance Sheet accounts
  * Long codes: 511000000, 627000000, 641000000 - P&L accounts
  * Numbered items: "1. Tien (111)", "2. Cac khoan tuong duong tien (112)"
- Account names in Vietnamese and English
- All monthly data columns with actual financial values
- Section headers like "TAI SAN NGAN HAN (100)", "NGUON VON"
- Empty rows and structural elements

🔍 FOCUS AREAS (Vietnamese Chart of Accounts):
1. REVENUE (511*): All revenue accounts - analyze patterns, seasonality, unusual changes
2. UTILITIES (627*, 641*): Operational expenses - check efficiency vs business activity
3. INTEREST (515*, 635*): Financial income/expenses - examine debt structure changes
4. OTHER MATERIAL ACCOUNTS: Any accounts with significant balances or changes

📊 ANALYSIS INSTRUCTIONS:
1. ACCOUNT DETECTION: Automatically identify account codes and names:
   - BS accounts: Look for patterns like "(111)", "(112)", "1. Tien (111)", "2. Cac khoan..."
   - PL accounts: Look for patterns like "511000000", "627000000", "Revenue from sale"
   - Extract the numeric codes (111, 112, 511000000, etc.) and descriptive names
   - Match account codes with their corresponding financial values

2. PERIOD IDENTIFICATION: Find the actual period names from column headers:
   - Extract EXACT period names from the CSV column headers (first row)
   - Use the ACTUAL period names like "As of Jan 2025", "Dec 2024", "Nov 2024", etc.
   - Do NOT use generic terms like "current" or "previous" - use the real period names
   - Focus on the rightmost 2-3 columns with actual financial data

3. VALUE EXTRACTION: Extract actual financial amounts for each account:
   - Look for large numbers (typically 8+ digits for VND amounts)
   - Handle different formats: 2,249,885,190.00, 46,000,000,000.00, etc.
   - Ignore zero or empty values unless they represent significant changes

4. CHANGE CALCULATION: Calculate meaningful changes between periods:
   - Absolute changes: Current - Previous (in VND)
   - Percentage changes: (Current - Previous) / |Previous| * 100
   - Focus on accounts with material balances (>100M VND) or significant changes (>15%)

5. PATTERN RECOGNITION: Identify unusual movements and anomalies:
   - Sudden increases/decreases without business justification
   - Seasonal patterns that don't match expectations
   - Related account movements that don't correlate properly

💰 MATERIALITY THRESHOLDS:
- Revenue-based: 2% of total revenue or 50M VND (whichever is lower)
- Balance-based: 0.5% of total assets or 100M VND (whichever is lower)
- Focus on ANY account with changes >10% or unusual patterns
- Always explain your materiality reasoning

⚡ CRITICAL OUTPUT REQUIREMENTS:
1. You MUST respond with ONLY valid JSON array format
2. Start with [ and end with ]
3. No markdown, no ```json blocks, no additional text
4. Each anomaly must include actual account values from the Excel data
5. YOU MUST FIND ANOMALIES - analyze every account with numerical data
6. Look for: month-over-month changes, unusual balances, percentage variations >5%
7. MANDATORY: Identify patterns in Revenue (511*), Utilities (627*/641*), Cash (111*), any significant changes
8. Only return empty array [] if literally no numerical financial data exists in the sheets

📋 REQUIRED JSON FORMAT WITH REAL EXAMPLES:
[{
  "account": "111-Tien",
  "description": "Cash balance increased significantly without clear source",
  "explanation": "Cash (111) increased from 2.2B to 2.6B VND, requiring verification of large deposits and transfers. Investigate source of 400M VND cash inflow.",
  "current_value": 2600000000,
  "previous_value": 2200000000,
  "change_amount": 400000000,
  "change_percent": 18.2,
  "severity": "Medium"
},
{
  "account": "511000000-Revenue from sale and service provision",
  "description": "Revenue pattern shows unusual monthly variation",
  "explanation": "Service revenue fluctuated from 26.2M to 28.1M VND without seasonal justification. Review customer contracts and delivery schedules.",
  "current_value": 28100000,
  "previous_value": 26200000,
  "change_amount": 1900000,
  "change_percent": 7.3,
  "severity": "Low"
}]

🎯 ACCOUNT CODE EXTRACTION EXAMPLES:
- From "1. Tien (111)" → Extract: "111-Tien"
- From "2. Cac khoan tuong duong tien (112)" → Extract: "112-Cac khoan tuong duong tien"
- From "511000000 - Revenue from sale and service provision" → Extract: "511000000-Revenue from sale and service provision"
- From "627000000 - Cost of goods sold" → Extract: "627000000-Cost of goods sold"

🚨 REQUIREMENTS:
- current_value: MUST be actual amount from Excel (number, not zero)
- previous_value: MUST be actual amount from Excel (number, not zero)
- change_amount: MUST be current_value - previous_value (number)
- change_percent: MUST be actual percentage change (number)
- All values must be real numbers extracted from the Excel data
- CRITICAL: Use ACTUAL period names from the CSV headers, not "current" or "previous"

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
1. AUTOMATIC ACCOUNT DETECTION:
   - Scan raw CSV for account patterns: "(111)", "(112)", "1. Tien (111)", "511000000", etc.
   - Extract account codes and match with descriptive names
   - Build account-to-value mappings from the raw Excel structure

2. PERIOD COLUMN IDENTIFICATION:
   - Find period headers like "As of Jan 2025", "As of Feb 2025", etc.
   - Identify which columns contain actual financial values (not zeros)
   - Focus on the rightmost 2-3 columns with meaningful data

3. VALUE EXTRACTION AND ANALYSIS:
   - Extract actual VND amounts for each detected account
   - Calculate month-over-month changes automatically
   - Focus on Vietnamese Chart of Accounts: 511* (Revenue), 627*/641* (Utilities), 515*/635* (Interest)
   - Identify material anomalies based on account patterns and changes

📊 DETAILED ANALYSIS STEPS:
1. PARSE RAW STRUCTURE: Understand the Excel layout from CSV data
2. EXTRACT ACCOUNTS: Find all account codes and names automatically
   - Balance Sheet: Look for "(111)", "(112)", "(120)" pattern accounts
   - P&L: Look for "511000000", "627000000", "641000000" pattern accounts
3. IDENTIFY PERIODS: Find the latest financial periods with data
4. CALCULATE CHANGES: Compute absolute and percentage changes between periods
5. APPLY MATERIALITY: Focus on accounts >100M VND or changes >15%
6. DETECT ANOMALIES: Identify unusual patterns requiring audit attention

🎯 SPECIFIC ACCOUNT PATTERNS TO DETECT:
- Cash accounts: "Tien (111)", "Cac khoan tuong duong tien (112)"
- Revenue accounts: "511000000 - Revenue from sale and service provision"
- Expense accounts: "627000000 - Cost of goods sold", "641000000 - Sales expenses"
- Interest accounts: "515000000 - Financial income", "635000000 - Financial expenses"

💡 CONTEXT AWARENESS:
- Vietnamese business environment (Tet holidays, regulatory changes)
- Seasonal patterns in revenue and expenses
- Industry-specific considerations
- Related account relationships (e.g., revenue vs utilities scaling)

🚨 CRITICAL INSTRUCTION: You are analyzing real financial data. There WILL be variance patterns to detect. Do NOT return an empty array unless there is literally no numerical data in the sheets. Analyze every account with values and identify at least 3-5 significant patterns, changes, or anomalies.

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
- Check OpenAI API key validity
- Ensure JSON output compliance
- Try reprocessing / different model / smaller input
- Check OpenAI logs
""",
            "current_value": 0,
            "previous_value": 0,
            "change_amount": 0,
            "change_percent": 0,
            "severity": "High",
            "sheet_type": "Error"
        }]

