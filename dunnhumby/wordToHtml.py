import mammoth
with open("C:/data/nadal.docx", "rb") as docx_file:
    result = mammoth.convert_to_html(docx_file)
with open("C:/data/nadal.html", "w") as html_file:
    html_file.write(result.value)