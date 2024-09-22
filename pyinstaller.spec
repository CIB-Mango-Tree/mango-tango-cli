# main.spec
# This file tells PyInstaller how to bundle your application

from PyInstaller.utils.hooks import copy_metadata

block_cipher = None

# inquirer depends on readchar as a hidden dependency that requires package metadata
datas = copy_metadata('readchar')

a = Analysis(
    ['mangotango.py'],  # Entry point
    pathex=['.'],    # Ensure all paths are correctly included
    binaries=[],
    datas=datas,          # Include any non-Python data files if needed
    hiddenimports=[
        'readchar',
        'numpy'
    ],  # Include any imports that PyInstaller might miss
    hookspath=[],
    runtime_hooks=[],
    excludes=[]
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    name='mangotango',  # The name of the executable
    debug=False,
    strip=True,
    upx=True,  # You can set this to False if you don’t want UPX compression
    console=True  # Set to False if you don't want a console window
)
