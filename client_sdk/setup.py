"""
Setup script for Policy Client SDK package.
"""
from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="policy-client-sdk",
    version="1.0.0",
    author="MLOps Pipeline Team",
    description="Policy enforcement client SDK for MLOps pipeline components",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/your-org/policy-service",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    python_requires=">=3.9",
    install_requires=[
        "aiohttp>=3.9.0",
        "cachetools>=5.0.0",
        "confluent-kafka>=2.0.0",
        "fastapi>=0.100.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.21.0",
            "httpx>=0.24.0",
        ],
    },
)
