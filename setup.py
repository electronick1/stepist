from setuptools import setup, find_packages


setup(
    name="stepist",
    version="0.0.6.1",
    author="Aleh Shydlouski",
    author_email="oleg.ivye@gmail.com",
    description="Data process utils",
    keywords=['data', 'ai', 'distribute'],
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'tqdm==4.25.0',
        'redis==3.0.1',
        'blinker==1.4',
        'click==7.0',
        'ujson==1.35',

    ],
    url='https://github.com/electronick1/stepist',
    download_url='https://github.com/electronick1/stepist/archive/0.0.6.tar.gz',
    classifiers=[],
)
