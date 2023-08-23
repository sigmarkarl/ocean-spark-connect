build:
	poetry build
	
publish:
	poetry publish -u __token__ -p $(PYPI_TOKEN)