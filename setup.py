import setuptools
# https://packaging.python.org/tutorials/packaging-projects/

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pysea-yandex-direct", # Replace with your own username
    version="0.0.5",
    author="Eugene Komissarov",
    author_email="ekom@cian.ru",
    description="Yandex Direct base",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://ekomissarov@bitbucket.org/ekomissarov/yandex_direct.git",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Linux",
    ],
    python_requires='>=3.7',
    install_requires=[
        'pysea-common-constants',
        'pysea-google-analytics',
        'certifi>=2020.6.20',
        'chardet>=3.0.4',
        'curlify>=2.2.1',
        'idna>=2.10',
        'requests>=2.24.0',
        'urllib3>=1.25.9',
    ]
)