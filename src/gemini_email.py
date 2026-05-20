# =========================================================
# GEMINI AI EMAIL DISCOVERY SERVICE
# =========================================================
import os
import json
import re
from typing import Optional, Dict
import google.generativeai as genai
from pydantic import BaseModel

class EmailDiscoveryResult(BaseModel):
    """Result from Gemini email discovery"""
    company_name: str
    company_domain: str
    career_page_url: str
    recruiter_email: str
    guessed: bool
    confidence: str  # high, medium, low
    reasoning: str

class GeminiEmailDiscovery:
    """Use Gemini AI to discover company emails from job descriptions"""
    
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY not found in environment")
        
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-1.5-flash')
    
    def _build_prompt(self, job_description: str, job_url: str = "") -> str:
        """Build the prompt for Gemini"""
        return f"""You are an expert AI email finder and recruiter contact discovery assistant.

Your goal is to extract or discover the best contact email address for job applications when a job posting does NOT list an email directly.

### Input Job Posting:
{job_description}

### Job URL (for context):
{job_url}

### Instructions:
1. Extract the **Company Name**, **Job Title**, and **Job Location** from the text.
2. Identify the most likely **official company domain** using reasoning and public info.
   Example: if the company is "Michael Page UAE", the domain might be "michaelpage.ae" or "michaelpage.com".
3. Find and return the **URL of the career or contact page**, prioritizing:
   - /careers
   - /contact
   - /jobs
   - /join-us
4. From the website or using logical patterns, find or generate the most probable **email address** to apply to:
   - hr@company.com
   - careers@company.com
   - jobs@company.com
   - recruitment@company.com
   - talent@company.com
5. If no valid email is public, generate an intelligent guess (e.g., careers@michaelpage.ae) and mark "guessed": true.

6. Return all results in this EXACT JSON format (no extra text, just JSON):
{{
  "company_name": "",
  "company_domain": "",
  "career_page_url": "",
  "recruiter_email": "",
  "guessed": true/false,
  "confidence": "high | medium | low",
  "reasoning": "Why this is likely correct"
}}

Rules:
* Use only company-related URLs (avoid GulfTalent, Indeed, Jooble, Adzuna, or external job board sites).
* Prefer .ae, .com, or regional domain relevant to the job's location.
* Avoid giving fake personal emails (like Gmail, Yahoo, etc.).
* Always return valid JSON.
* If the company is very small or unknown, confidence should be "low" and guessed should be true.

Now extract recruiter contact information for the above job posting. Return ONLY the JSON, no other text."""

    def discover_email(self, job_description: str, job_url: str = "") -> Optional[EmailDiscoveryResult]:
        """
        Use Gemini to discover company email from job description
        
        Args:
            job_description: Full text of the job posting
            job_url: URL of the job (for additional context)
            
        Returns:
            EmailDiscoveryResult or None if failed
        """
        try:
            # Build prompt
            prompt = self._build_prompt(job_description, job_url)
            
            # Call Gemini
            response = self.model.generate_content(prompt)
            response_text = response.text.strip()
            
            # Extract JSON from response (sometimes Gemini adds markdown)
            json_match = re.search(r'\{[\s\S]*\}', response_text)
            if json_match:
                json_text = json_match.group(0)
            else:
                json_text = response_text
            
            # Parse JSON
            result_data = json.loads(json_text)
            
            # Validate and create result
            result = EmailDiscoveryResult(**result_data)
            
            return result
            
        except json.JSONDecodeError as e:
            print(f"❌ Gemini JSON parse error: {e}")
            print(f"Response was: {response_text[:500]}")
            return None
        except Exception as e:
            print(f"❌ Gemini discovery error: {e}")
            return None
    
    def discover_email_with_retry(
        self, 
        job_description: str, 
        job_url: str = "",
        max_retries: int = 2
    ) -> Optional[EmailDiscoveryResult]:
        """
        Discover email with retry logic
        """
        for attempt in range(max_retries):
            try:
                result = self.discover_email(job_description, job_url)
                if result and result.recruiter_email and '@' in result.recruiter_email:
                    return result
                print(f"⚠️ Attempt {attempt + 1} failed, retrying...")
            except Exception as e:
                print(f"⚠️ Attempt {attempt + 1} error: {e}")
        
        return None


# =========================================================
# USAGE EXAMPLE
# =========================================================

async def discover_company_email_example():
    """Example usage of Gemini email discovery"""
    
    job_description = """
    Treasury Analyst
    Michael Page
    Dubai, United Arab Emirates
    
    About the job:
    Our client is seeking a skilled Treasury Analyst to join their finance team in Dubai.
    
    Key Responsibilities:
    - Cash management and forecasting
    - Treasury operations support
    - Banking relationship management
    
    Requirements:
    - Bachelor's degree in Finance or Accounting
    - 3+ years of treasury experience
    - Strong Excel skills
    
    About Michael Page:
    Michael Page is a leading global recruitment consultancy with offices across the Middle East.
    """
    
    # Initialize service
    discovery = GeminiEmailDiscovery()
    
    # Discover email
    result = discovery.discover_email_with_retry(
        job_description=job_description,
        job_url="https://www.gulftalent.com/uae/jobs/treasury-analyst-123456"
    )
    
    if result:
        print("✅ Email Discovery Result:")
        print(f"  Company: {result.company_name}")
        print(f"  Domain: {result.company_domain}")
        print(f"  Email: {result.recruiter_email}")
        print(f"  Confidence: {result.confidence}")
        print(f"  Guessed: {result.guessed}")
        print(f"  Reasoning: {result.reasoning}")
    else:
        print("❌ Failed to discover email")


# =========================================================
# FALLBACK: Simple Email Guessing
# =========================================================

def guess_company_email(company_name: str, location: str = "") -> Optional[str]:
    """
    Fallback: Simple rule-based email guessing if Gemini fails
    """
    # Clean company name
    company_clean = company_name.lower().strip()
    company_clean = re.sub(r'[^a-z0-9]', '', company_clean)
    
    # Determine domain extension based on location
    if 'uae' in location.lower() or 'dubai' in location.lower() or 'emirates' in location.lower():
        extensions = ['.ae', '.com']
    else:
        extensions = ['.com', '.ae']
    
    # Common HR email patterns
    patterns = [
        f"careers@{company_clean}",
        f"hr@{company_clean}",
        f"jobs@{company_clean}",
        f"recruitment@{company_clean}",
        f"talent@{company_clean}"
    ]
    
    # Generate guesses
    guesses = []
    for pattern in patterns:
        for ext in extensions:
            guesses.append(pattern + ext)
    
    # Return best guess
    return guesses[0] if guesses else None


if __name__ == "__main__":
    import asyncio
    asyncio.run(discover_company_email_example())