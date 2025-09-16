# Security Guidelines

## Credential Management

### Never Commit Credentials
- **NEVER** commit passwords, API keys, or sensitive data to version control
- All credentials must be stored in environment variables or `.env` files
- `.env` files must be listed in `.gitignore`

### Environment Variables
Use the `.env.example` template to create your local `.env` file:
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

### Configuration Files
- Production config files (`*_config.yaml`) are gitignored
- Only example configs (`*_config.example.yaml`) are committed
- Copy example configs and add your credentials locally:
```bash
cp data_collector_config.example.yaml data_collector_config.yaml
# Edit data_collector_config.yaml with your credentials
```

## Database Security

### User Permissions
We use a three-tier PostgreSQL user system:

1. **trading_admin**: Full database access (migrations only)
2. **trading_writer**: INSERT/UPDATE/SELECT/DELETE (data collection)
3. **trading_reader**: SELECT only (analysis and reporting)

### Best Practices
- Use the appropriate user for each task
- Never share admin credentials
- Rotate passwords every 3-6 months
- Monitor database connections for unusual activity
- Restrict database access by IP if possible

## API Key Security

### Bybit API Keys
- Use read-only API keys when possible
- Never commit API keys to the repository
- Store API keys in environment variables:
```python
import os
api_key = os.getenv('BYBIT_API_KEY')
api_secret = os.getenv('BYBIT_API_SECRET')
```

## VPS Security

### SSH Access
- Use SSH keys instead of passwords
- Disable root login
- Use non-standard SSH port
- Enable firewall (ufw)
- Keep system updated

### Database Access
- PostgreSQL should only accept connections from specific IPs
- Use SSL/TLS for remote database connections
- Regular security updates

## Development Security

### Before Committing
1. Check for passwords: `grep -r "password" --include="*.py" --include="*.yaml"`
2. Review changed files: `git diff --staged`
3. Ensure `.gitignore` is properly configured
4. Never use real credentials in tests

### Code Review Checklist
- [ ] No hardcoded passwords
- [ ] No API keys in code
- [ ] Sensitive files are gitignored
- [ ] Environment variables used for credentials
- [ ] No credentials in logs or debug output

## Incident Response

If credentials are accidentally committed:
1. Immediately rotate the exposed credentials
2. Remove the commit from history using `git filter-branch` or BFG
3. Force push the cleaned history
4. Notify team members to re-clone the repository
5. Audit systems for any unauthorized access

## Monitoring

### Regular Audits
- Weekly: Check for exposed credentials in code
- Monthly: Review database user permissions
- Quarterly: Rotate passwords and API keys
- Ongoing: Monitor logs for suspicious activity

### Automated Checks
Consider using tools like:
- `git-secrets` to prevent credential commits
- `truffleHog` to scan for secrets in git history
- GitHub secret scanning (if using GitHub)

## Contact

For security concerns or to report vulnerabilities, contact the project maintainer immediately.

---
Last Updated: 2025-09-16