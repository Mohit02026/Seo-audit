FROM python:3.11-slim

# System deps for Playwright browsers (Debian trixie names)
RUN apt-get update && apt-get install -y \
    curl wget gnupg ca-certificates \
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libxrender1 libxshmfence1 libasound2 \
    libpango-1.0-0 libpangocairo-1.0-0 libcairo2 \
    libglib2.0-0 libgdk-pixbuf-xlib-2.0-0 libx11-6 libx11-xcb1 \
    libxcb1 libxext6 libxi6 libxtst6 libgbm1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright browsers
RUN python -m playwright install --with-deps chromium

# Copy the rest of the app
COPY . .

# Expose port
EXPOSE 8000

# Run FastAPI app
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]