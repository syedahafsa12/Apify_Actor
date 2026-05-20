# =========================================================
# temp_email.py - Temporary Email Service
# =========================================================
import httpx
import asyncio
import random
import string
from typing import Optional, Dict

class TempEmailService:
    """
    Generates temporary email addresses for automation.
    Uses 1secmail.com API (no auth required).
    """
    
    def __init__(self):
        self.base_url = "https://www.1secmail.com/api/v1/"
        self.email = None
        self.login = None
        self.domain = None
    
    async def create_temp_email(self) -> str:
        """Generate a random temporary email address"""
        try:
            # Generate random login
            self.login = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            
            # Get available domains
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(f"{self.base_url}?action=getDomainList")
                domains = response.json()
                self.domain = random.choice(domains)
            
            self.email = f"{self.login}@{self.domain}"
            print(f"[TempEmail] ✅ Created: {self.email}")
            return self.email
            
        except Exception as e:
            print(f"[TempEmail] ❌ Creation failed: {e}")
            # Fallback to hardcoded domain
            self.login = ''.join(random.choices(string.ascii_lowercase + string.digits, k=10))
            self.domain = "1secmail.com"
            self.email = f"{self.login}@{self.domain}"
            return self.email
    
    async def check_inbox(self, wait_seconds: int = 30) -> Optional[Dict]:
        """
        Check for new emails (for email verification).
        Returns first email with verification link if found.
        """
        if not self.email:
            return None
        
        try:
            print(f"[TempEmail] 📬 Checking inbox for {self.email}...")
            
            for _ in range(wait_seconds):
                async with httpx.AsyncClient(timeout=30) as client:
                    response = await client.get(
                        f"{self.base_url}?action=getMessages&login={self.login}&domain={self.domain}"
                    )
                    messages = response.json()
                    
                    if messages:
                        # Get first message
                        msg_id = messages[0]['id']
                        msg_response = await client.get(
                            f"{self.base_url}?action=readMessage&login={self.login}&domain={self.domain}&id={msg_id}"
                        )
                        message = msg_response.json()
                        print(f"[TempEmail] ✅ Received email: {message.get('subject', 'No subject')}")
                        return message
                
                await asyncio.sleep(1)
            
            print(f"[TempEmail] ⏱️ No emails received in {wait_seconds}s")
            return None
            
        except Exception as e:
            print(f"[TempEmail] ❌ Inbox check failed: {e}")
            return None
    
    def extract_verification_link(self, message: Dict) -> Optional[str]:
        """Extract verification/confirmation link from email body"""
        if not message:
            return None
        
        try:
            body = message.get('body', '') or message.get('htmlBody', '')
            
            # Common verification link patterns
            patterns = [
                r'https?://[^\s<>"]+/verify[^\s<>"]*',
                r'https?://[^\s<>"]+/confirm[^\s<>"]*',
                r'https?://[^\s<>"]+/activate[^\s<>"]*',
                r'https?://[^\s<>"]+token=[^\s<>"]+',
            ]
            
            import re
            for pattern in patterns:
                match = re.search(pattern, body)
                if match:
                    link = match.group(0)
                    print(f"[TempEmail] 🔗 Found verification link: {link[:50]}...")
                    return link
            
            return None
            
        except Exception as e:
            print(f"[TempEmail] ❌ Link extraction failed: {e}")
            return None

async def create_temp_email() -> tuple[str, TempEmailService]:
    """
    Helper function to create a temp email.
    Returns (email_address, service_instance)
    """
    service = TempEmailService()
    email = await service.create_temp_email()
    return email, service