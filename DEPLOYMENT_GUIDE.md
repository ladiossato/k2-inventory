# K2 Inventory System - Deployment Guide

## Quick Start

1. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure Environment**
   ```bash
   cp .env.template .env
   # Edit .env with your Telegram bot token and chat IDs
   ```

3. **Run Application**
   ```bash
   streamlit run k2_inventory_app.py
   ```

## Mobile Access

The app is optimized for mobile devices. Access via:
- **Local**: `http://localhost:8501` on your phone (same WiFi network)
- **Network**: `http://YOUR_IP:8501` 
- **Cloud**: Deploy to Streamlit Cloud, Heroku, or similar

## Production Deployment

### Option 1: Streamlit Cloud (Recommended)
1. Push code to GitHub repository
2. Connect to [share.streamlit.io](https://share.streamlit.io)
3. Add secrets in Streamlit Cloud dashboard:
   ```toml
   TELEGRAM_BOT_TOKEN = "your_bot_token"
   CHAT_ONHAND = "-1002819958218"
   CHAT_AUTOREQUEST = "-1002819958218" 
   CHAT_RECEIVED = "-4957164054"
   CHAT_REASSURANCE = "6904183057"
   TZ = "America/Chicago"
   ```

### Option 2: VPS/Server Deployment
1. **Setup systemd service** (Ubuntu/CentOS):
   ```bash
   sudo cp k2-inventory.service /etc/systemd/system/
   sudo systemctl enable k2-inventory
   sudo systemctl start k2-inventory
   ```

2. **Nginx reverse proxy** (optional):
   ```nginx
   server {
       listen 80;
       server_name your-domain.com;
       
       location / {
           proxy_pass http://127.0.0.1:8501;
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
       }
   }
   ```

### Option 3: Docker Deployment
```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8501

CMD ["streamlit", "run", "k2_inventory_app.py", "--server.address", "0.0.0.0"]
```

## Configuration

### Telegram Bot Setup
1. Create bot with [@BotFather](https://t.me/BotFather)
2. Get bot token
3. Add bot to your groups/channels
4. Get chat IDs (use [@userinfobot](https://t.me/userinfobot))

### Test vs Production Mode
- Set `USE_TEST_CHAT = True` for testing
- Set `USE_TEST_CHAT = False` for production
- All test messages go to your personal chat

### Database
- SQLite database created automatically
- Location: `k2.db` in app directory
- Automatic 3-month data retention
- WAL mode for better concurrency

## Features Overview

### 📱 Entry Page (Mobile-Optimized)
- Quick data entry for On-Hand, Received, Request
- Touch-friendly interface
- Real-time validation
- Telegram notifications

### 📊 Analytics Dashboard
- Historical trends and charts
- Status distribution analytics
- Entry log with filtering
- Data export (CSV/Excel)
- Manager performance tracking

### ⚙️ Admin Settings
- Edit items (ADU, par levels, case sizes)
- Add/remove inventory items
- System health monitoring
- Scheduler management
- Data cleanup tools

## Scheduled Jobs

- **Auto-Request**: Tue/Sat 8:00 AM
- **Reassurance**: Daily 5:00 PM
- **Missing Counts**: Daily 11:59 PM
- **Data Cleanup**: Daily 2:00 AM

## Mobile Optimization

- Responsive design (mobile-first)
- Large touch targets
- Streamlined navigation
- Fast loading on mobile data
- Progressive Web App capabilities

## Troubleshooting

### Common Issues
1. **Scheduler not running**: Restart from Admin → System Health
2. **Telegram not working**: Check bot token and chat IDs
3. **Database locked**: Restart application
4. **Mobile layout issues**: Clear browser cache

### Logs
Check console output for detailed logging:
```bash
streamlit run k2_inventory_app.py 2>&1 | tee k2.log
```

### Recovery Mode
If system crashes, use "Entry Only Mode" button for emergency data entry.

## Support

For issues or enhancements:
1. Check logs for error details
2. Test with fresh `.env` file
3. Verify Telegram bot permissions
4. Check database file permissions

## Backup Strategy

### Database Backup
```bash
# Backup
cp k2.db k2_backup_$(date +%Y%m%d).db

# Restore
cp k2_backup_20241201.db k2.db
```

### Export Data
Use Admin → System Settings → Export All Data for comprehensive backup.

## Security Notes

- Never commit `.env` file
- Rotate Telegram bot token after testing
- Use HTTPS in production
- Restrict database file permissions
- Regular security updates