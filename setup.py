import os
from setuptools import setup, find_packages

__version__ = '1.0.1'

# We use the README as the long_description
readme_path = os.path.join(os.path.dirname(__file__), "README.rst")


setup(
    name='asgi_amqp',
    version=__version__,
    url='http://github.com/ansible/asgi_amqp/',
    author='Wayne Witzel III',
    author_email='wayne@riotousliving.com',
    description='AMQP-backed ASGI channel layer implementation',
    long_description=open(readme_path).read(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: BSD License',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ],
    keywords='asgi_amqp asgi amqp rabbitmq django channels',
    license='BSD',
    zip_safe=False,
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'six',
        'kombu>=3.0.35',
        'msgpack-python>=0.4.7',
        'asgiref==1.1.2',
        'jsonpickle>=0.9.3',
    ],
    tests_require=['pytest'],
)
