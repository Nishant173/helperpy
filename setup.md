# Setup
- Use Python version 3.6 or above
- Create virtual environment
- Install dependencies via: `pip install -r requirements.txt`
- Create documentation for the package with PyDoc via: `pdoc --html <package_name>`
- Publish the package to PyPi with the following commands
    - `py setup.py sdist bdist_wheel`
    - `twine upload dist/*` OR `twine upload dist/* --verbose`
    - Enter your PyPi username and password
