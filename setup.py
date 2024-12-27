from setuptools import setup, find_packages

setup(
    name="maazDB",  # The name of your package
    version="0.2.1",  # The version of your library
    author="Maaz Waheed",  # Your name
    author_email="maazw435@gmail.com",  # Your email address
    description="A lightweight key-value database library using pickle",  # Short description
    long_description=open('README.md').read(),  # Long description from the README file
    long_description_content_type="text/markdown",  # Markdown format for the long description
    url="https://github.com/Maaz-waheed/maazDB",  # Replace with your repository URL
    packages=find_packages(),  # Automatically find and include all packages in your project
    python_requires=">=3.6",  # Minimum Python version required
    classifiers=[
        "Programming Language :: Python :: 3",  # Python version
        "License :: OSI Approved :: MIT License",  # License type
        "Operating System :: OS Independent",  # Works across operating systems
    ],
    install_requires=[],  # List any dependencies here (if any)
)
