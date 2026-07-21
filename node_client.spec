# -*- mode: python ; coding: utf-8 -*-
# Build the donor node client into a single console exe:
#   pyinstaller --noconfirm --clean node_client.spec
# Ship dist\UbiTokeerNode.exe together with node_config.json to the donor.
from PyInstaller.utils.hooks import collect_all

datas = []
binaries = []
hiddenimports = []

# pywinpty ships a native agent + dll that PyInstaller won't find on its own.
for pkg in ("winpty",):
    d, b, h = collect_all(pkg)
    datas += d
    binaries += b
    hiddenimports += h

a = Analysis(
    ['node_client.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports + ['winpty', 'core.cli_worker'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['customtkinter', 'fastapi', 'uvicorn', 'tkinter', 'matplotlib', 'numpy'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='UbiTokeerNode',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,          # donor watches the log output
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
