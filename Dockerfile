FROM apify/actor-python-playwright:3.13

# Install system dependencies as root
USER root
RUN apt-get update && apt-get install -y \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

# Switch to myuser
USER myuser

ENV PLAYWRIGHT_BROWSERS_PATH=/home/myuser/.cache/ms-playwright
ENV PYTHONUNBUFFERED=1

# Install Python dependencies
COPY --chown=myuser:myuser requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium browser
RUN python -m playwright install chromium
RUN pip install --no-cache-dir google-generativeai


# Copy source code
COPY --chown=myuser:myuser ./src ./src

# Run the actor
CMD ["python3", "-m", "src"]