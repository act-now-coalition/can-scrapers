sync_services:
	sudo mkdir -p /usr/lib/systemd/system/
	sudo rm -f /usr/lib/systemd/system/prefect-server.service
	sudo cp ./prefect-server.service /usr/lib/systemd/system/prefect-server.service
	sudo rm -f /usr/lib/systemd/system/prefect-agent.service
	sudo cp ./prefect-agent.service /usr/lib/systemd/system/prefect-agent.service
		sudo systemctl daemon-reload

start_services:
	sudo systemctl enable --now prefect-server
	sudo systemctl enable --now prefect-agent

restart_services:
	sudo systemctl restart prefect-agent
	sudo systemctl restart prefect-server

