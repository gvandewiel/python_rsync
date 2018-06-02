from setuptools import setup
setup(
    name = 'pyrsync',
    version = '0.1.0',
    packages = ['pyrsync'],
    entry_points = {
        'console_scripts': [
            'pyrsync = pyrsync.__main__:main'
        ]
    })
