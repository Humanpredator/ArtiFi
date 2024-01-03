from setuptools import find_packages, setup

setup(
    name="Artifi",  # Replace with your package name
    version="0.1.0",  # Initial version, follow Semantic Versioning (SemVer)
    author="Humanpredator",
    author_email="admin@reality.org.in",
    description="A Automation Tool Made By Noob",
    long_description="Collection of Some Random Api, FAFO",
    long_description_content_type="text/markdown",
    url="https://github.com/Humanpredator/ArtiFi",  # Replace with your repository URL
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Programming Language :: Python :: 3.10",
    ],
    python_requires=">=3.10",
    install_requires=['aiohttp==3.9.1', 'aiosignal==1.3.1', 'APScheduler==3.10.4',
                      'async-timeout==4.0.3', 'attrs==23.1.0', 'blinker==1.7.0',
                      'cachetools==5.3.2', 'certifi==2023.11.17', 'cffi==1.16.0',
                      'charset-normalizer==3.3.2', 'click==8.1.7', 'colorama==0.4.6',
                      'discord==2.3.2', 'discord.py==2.3.2', 'Flask==3.0.0',
                      'frozenlist==1.4.1', 'google-api-core==2.15.0',
                      'google-api-python-client==2.111.0', 'google-auth==2.25.2',
                      'google-auth-httplib2==0.2.0', 'google-auth-oauthlib==1.2.0',
                      'googleapis-common-protos==1.62.0', 'greenlet==3.0.1',
                      'httplib2==0.22.0', 'idna==3.6', 'instaloader==4.10.2',
                      'itsdangerous==2.1.2', 'Jinja2==3.1.2', 'MarkupSafe==2.1.3',
                      'multidict==6.0.4', 'oauthlib==3.2.2', 'playwright==1.40.0',
                      'protobuf==4.25.1', 'psutil==5.9.7', 'py-cpuinfo==9.0.0',
                      'pyasn1==0.5.1', 'pyasn1-modules==0.3.0', 'pycparser==2.21',
                      'pyee==11.0.1', 'PyNaCl==1.5.0', 'pyparsing==3.1.1',
                      'python-dotenv==1.0.0', 'python-magic==0.4.27',
                      'pytz==2023.3.post1', 'requests==2.31.0',
                      'requests-oauthlib==1.3.1', 'rsa==4.9', 'six==1.16.0',
                      'speedtest-cli==2.1.3', 'SQLAlchemy==2.0.23', 'tenacity==8.2.3',
                      'typing_extensions==4.9.0', 'tzdata==2023.3', 'tzlocal==5.2',
                      'uritemplate==4.1.1', 'urllib3==2.1.0', 'wavelink==3.1.0',
                      'Werkzeug==3.0.1', 'yarl==1.9.2']

)
