module.exports = {
  apps: [
    {
      name: 'deriv-trading-backend',
      script: 'dist/server.js',
      // 1 processo: sessões, bots e catálogo vivem em memória — cluster partia tudo.
      instances: 1,
      exec_mode: 'fork',
      watch: false,
      env: {
        NODE_ENV: 'development',
      },
      env_production: {
        NODE_ENV: 'production',
      },
      error_file: 'logs/error.log',
      out_file: 'logs/out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      max_memory_restart: '512M',
      listen_timeout: 10000,
      kill_timeout: 10000,
      wait_ready: true,
      shutdown_with_message: true,
      ignore_watch: ['node_modules', 'logs', 'dist'],
      max_restarts: 10,
      min_uptime: '10s',
      autorestart: true,
    },
  ],
};
