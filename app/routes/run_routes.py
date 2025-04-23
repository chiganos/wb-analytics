from fastapi import APIRouter
import subprocess

run_router = APIRouter()

@run_router.get("/run/funnel")
def run_funnel_parser():
    try:
        result = subprocess.run(["python", "app/parsers/funnel_parser.py"], capture_output=True, text=True)
        return {"stdout": result.stdout, "stderr": result.stderr}
    except Exception as e:
        return {"error": str(e)}

@run_router.get("/run/positions")
def run_positions_parser():
    try:
        result = subprocess.run(["python", "app/parsers/positions_parser.py"], capture_output=True, text=True)
        return {"stdout": result.stdout, "stderr": result.stderr}
    except Exception as e:
        return {"error": str(e)}

@run_router.get("/run/ads")
def run_ads_parser():
    try:
        result = subprocess.run(["python", "app/parsers/ads_parser.py"], capture_output=True, text=True)
        return {"stdout": result.stdout, "stderr": result.stderr}
    except Exception as e:
        return {"error": str(e)}
