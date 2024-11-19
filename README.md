# Telegram Data Collection

A Telegram client application for collecting and processing channel messages using the gotd/td library.

## Prerequisites

- Go 1.19 or higher
- Make

## Setup

1. Copy the environment variables template:
```bash
cp .env.example .env
```

2. Get your Telegram API credentials:
   - Visit https://my.telegram.org/
   - Log in with your phone number
   - Go to 'API development tools'
   - Create a new application
   - Copy the `api_id` and `api_hash`

3. Configure your `.env` file with:
   - Your phone number (international format, e.g., +1234567890)
   - Your API ID from my.telegram.org
   - Your API hash from my.telegram.org

## Configuration

Add your whitelisted channels to the `.env` file:

```env
TELEGRAM_CHANNEL_WHITELIST=@testchannelcollect,123456789
```

You can specify channels using either:
- Channel usernames (starting with @)
- Numeric channel IDs

The application will automatically resolve usernames to channel IDs during startup.

## Running the Application

1. Start the application:
```bash
make telegram
```

2. First-time Authentication:
   - Enter the verification code sent to your Telegram
   - If 2FA is enabled, enter your password when prompted
   - Sessions are saved locally for future use

## Features

- Automatic session management
- Update recovery system
- Flood wait handling
- Rate limiting
- Peer storage caching