from setuptools import setup, find_packages


setup(
    name="stepist",
    version="0.0.1",
    author="Aleh Shydlouski",
    author_email="oleg.ivye@gmail.com",
    description="Data process utils",
    keywords=['data', 'ai', 'distribute'],
    packages=find_packages(),
    install_requires=[
        'tqdm',
        'redis',
        'blinker',
    ],
    url='https://github.com/electronick1/stepist',
    download_url='https://github.com/electronick1/stepist/archive/0.1.tar.gz',
    classifiers=[],
)
