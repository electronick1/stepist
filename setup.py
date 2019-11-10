from setuptools import setup, find_packages


setup(
    name="stepist",
    version="0.1.6.1",
    author="Aleh Shydlouski",
    author_email="oleg.ivye@gmail.com",
    description="Data process utils",
    keywords=['data', 'ai', 'distribute'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'tqdm',
        'redis >= 3.0.0',
        'blinker',
        'click',
        'ujson>=1.0',

    ],
    url='https://github.com/electronick1/stepist',
    download_url='https://github.com/electronick1/stepist/archive/0.1.6.1.tar.gz',
    classifiers=[],
)
