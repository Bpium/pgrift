module.exports = {
  apps: [
    {
      name: 'pgrift',
      script: 'npx',
      args: 'tsx migrate.ts',
      interpreter: 'none',
      autorestart: false, // migration runs once, no need to restart
      watch: false,
      out_file: './logs/pgrift.log',
      error_file: './logs/pgrift.error.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss',
      env: {
        NODE_ENV: 'production',
      },
    },
  ],
};
