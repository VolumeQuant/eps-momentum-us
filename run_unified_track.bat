@echo off
cd /d "C:\dev\claude code\eps-momentum-us"
git pull --ff-only origin master >nul 2>&1
"C:\Users\user\miniconda3\envs\volumequant\python.exe" unified_vm_track.py --run >> logs\unified_track.log 2>&1
