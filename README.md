# CVAT Dataset Converter Utility – Frontend

It provides a small, self-contained UI to prepare CVAT exports (CVAT for images 1.1) for conversion into downstream formats (YOLO / Pascal VOC / TAO) and related operations.

---

## Folder Structure

```text
frontend/
  index.html
  css/
    styles.css
  js/
    session.js
    api.js
    dom.js
    app.js
```

## How to Run the Frontend

We will use python `http.server`
```sh
cd frontend
python -m http.server 8081
```

Then open
```text
http://localhost:8081/
```