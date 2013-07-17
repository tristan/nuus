setup(name='Nuus',
      version='0.1',
      description='Newsgroup Indexer',
      author='Tristan King',
      author_email='tristan.king@gmail.com',
      url='http://github.com/tristan/nuus',
      packages=['nuus'],
      install_requires=[
          'redis',
          'hiredis',
          'flask',
          'tornado'
      ]
     )
