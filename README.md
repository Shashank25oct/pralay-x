# PRALAY X v36 — Final Report Print Fix

Final prototype pack based on v34.

https://pralay-x-flame.vercel.app/

### Report fixes in v36
- Fixed official report printing so the report is no longer clipped by the modal scroll viewport.
- Print / Save PDF now prints the complete report from the top, including the PRALAY X logo, report metadata, KPI/stat cards, map snapshot, incident situation, risk & prediction, alerts, evacuation/shelter information, response routes/resources, and verification/audit note.
- Reduced the report map snapshot size so it does not dominate the report page.
- Added A4 print sizing and page-break controls for cleaner multi-page PDF output.
- Downloaded HTML report also uses a smaller map and print-friendly layout.

### Run
Frontend:
```powershell
cd frontend
npm install --legacy-peer-deps
npm run dev
```

Backend:
```powershell
cd backend
uvicorn app.main:app --reload --port 8000
```

### Demo authority
- Email: `admin@pralayx.gov.in`
- Password: `PralayX@123`

> Simulation/seeded data is clearly distinguished from operational records. Critical actions remain under authorized human control.

## Copyright

Copyright © 2026 Shashank Shrivastava.

PRALAY X is an open-source disaster intelligence and early-warning
platform developed by Shashank Shrivastava.

## License

PRALAY X is released under the MIT License.

See the [LICENSE](LICENSE) file for the complete license text.