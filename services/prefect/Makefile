sync_services:
	sudo rm -f /etc/systemd/system/prefect-agent.service
	sudo cp prefect-agent.service /etc/systemd/system/prefect-agent.service
	sudo systemctl daemon-reload

start_services:
	sudo systemctl start prefect-agent.service
	sudo systemctl enable prefect-agent.service

restart_services:
	sudo systemctl restart prefect-agent.service

setup_nginx:
	sudo rm -f /etc/nginx/sites-enabled/prefect
	sudo cp ./nginx.conf /etc/nginx/sites-enabled/prefect
	sudo systemctl restart nginx

