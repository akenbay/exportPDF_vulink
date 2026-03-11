from services.report_service import generate_report_bytes

buffer = generate_report_bytes(
    locations=["60 к накопитель"],
    date_from="2026-02-09",
    date_to="2026-03-11"
)

with open("test_output.pdf", "wb") as f:
    f.write(buffer.read())

print("✅ PDF saved to test_output.pdf")
