# Nuus

## setup

    virtualenv .
    source bin/activate # or on windows: scripts\activate.bat
    pip install flask tornado redis hiredis
    python -c "import os; f = open(os.path.abspath(os.path.join(os.__file__, '..', 'site-packages', 'nuus.pth')), 'w'); f.write(os.path.abspath('.')); f.close()"

## run server

    python nuus

## indexer

    python indexer.py