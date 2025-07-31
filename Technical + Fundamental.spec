# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['Technical + Fundamental.py'],
    pathex=[],
    binaries=[],
    datas=[('the-money-method-ad6d7-f95331cf5cbf.json', '.'), ('moneycontrol_scraper.js', '.'), ('nodejs-portable\\\\node.exe', 'nodejs-portable'), ('nodejs-portable\\\\node_modules', 'nodejs-portable/node_modules')],
    hiddenimports=[],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='Technical + Fundamental',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='Technical + Fundamental',
)
