from setuptools import setup, find_packages

develop = set([
    'ipython',
    'pytest',
    'setuptools',
    'twine',
    'wheel',
])

runtime = set([
    "attrs",
    "insights-core",
    "insights-core-messaging",
    "app-common-python",
    "requests",
    "s3fs",
    "retry",
])

kafka = set([
    "confluent-kafka"
])

rabbitmq = set([
    "pika",
])

if __name__ == "__main__":
    setup(
        name="ccx-messaging",
        version="1.0.0",
        description="ICM components used in CCX data pipeline",
        long_description=open("README.md").read(),
        long_description_content_type='text/markdown',
        url="https://github.com/RedHatInsights/insights-ccx-messaging",
        packages=find_packages(),
        package_data={'': ['LICENSE']},
        license='Apache 2.0',
        install_requires=list(runtime),
        extras_require={
            'develop': list(develop),
            'kafka': list(kafka),
            'rabbitmq': list(rabbitmq),
        },
        classifiers=[
            'Intended Audience :: Developers',
            'Natural Language :: English',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7'
        ],
        include_package_data=True
    )
