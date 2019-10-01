import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name='dynamics',
    version='0.3.1',
    scripts=[
        'objects.py',
        'utils.py'
    ],
    author="Alexey Shytikov",
    author_email="alexey.shytikov@gmail.com",
    description="A pandas helper to interact with MS Dynamics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/shytikov/dynamics",
    install_requires=[
        'adal',
        'pandas',
        'requests',
    ],
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
