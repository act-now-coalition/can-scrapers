sync:
		sudo mkdir -p /usr/lib/systemd/system/
		sudo rm -f /usr/lib/systemd/system/webhook.service
		sudo cp ./webhook.service /usr/lib/systemd/system/webhook.service
		sudo systemctl daemon-reload

start:
		sudo systemctl enable --now webhook

restart:
		sudo systemctl restart webhook

setup_nginx:
		sudo ln -s ./nginx.conf /etc/nginx/sites-enabled/webhook
		sudo systemctl restart nginx
