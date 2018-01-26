from distutils.core import setup
setup(
  name = 'nem',
  packages = ['nem'], # this must be the same as the name above
  version = '0.1',
  description = 'Access library for AEMO / NEM goverement data',
  author = 'Michael Wheeler',
  author_email = 'michael@michael-wheeler.org',
  url = 'https://github.com/theskorm/nemweb2', # use the URL to the github repo
  download_url = 'https://github.com/TheSkorm/nemweb2/archive/0.0.1.tar.gz', # I'll explain this in a second
  keywords = ['aemo', 'nem', 'gov', 'au'], # arbitrary keywords
  classifiers = [],
  install_requires = [
        "beautifulsoup4",
        "boto3",
        "arrow",
        "sphinx_rtd_theme"
  ]
)