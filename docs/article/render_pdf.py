"""Render a readable two-column PDF when a local TeX distribution is unavailable."""

from __future__ import annotations

import re
from pathlib import Path
from xml.sax.saxutils import escape

from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import BaseDocTemplate, Frame, PageBreak, PageTemplate, Paragraph, Spacer


ROOT = Path(__file__).parent
SOURCE = ROOT / "fraud_lakehouse_article.tex"
OUTPUT = ROOT / "fraud_lakehouse_article.pdf"


def strip_latex(value: str) -> str:
    value = re.sub(r"\\cite\{([^}]*)\}", r"[\1]", value)
    value = re.sub(r"\\(?:texttt|textit|textbf|emph|url)\{([^}]*)\}", r"\1", value)
    value = re.sub(r"\\label\{[^}]*\}", "", value)
    value = re.sub(r"\\caption\{([^}]*)\}", r"Tabela/Figura: \1", value)
    value = re.sub(r"\\(?:begin|end)\{[^}]*\}", "", value)
    value = re.sub(r"\\bibitem\{([^}]*)\}", r"[\1]", value)
    value = re.sub(r"\\(?:toprule|midrule|bottomrule|centering|small|maketitle)", "", value)
    value = re.sub(r"\\(?:fbox|parbox)\{[^}]*\}", "", value)
    value = value.replace("\\\\", " ")
    value = value.replace("\\%", "%").replace("\\&", "&")
    value = re.sub(r"\\[a-zA-Z]+", "", value)
    value = re.sub(r"\\['`^\"~=.]?\{?([A-Za-z])\}?", r"\1", value)
    value = value.replace("$", "").replace("~", " ")
    value = value.replace("{", "").replace("}", "")
    return re.sub(r"\s+", " ", value).strip()


def page(canvas, document):
    canvas.saveState()
    canvas.setStrokeColor(HexColor("#777777"))
    canvas.setLineWidth(0.3)
    canvas.line(1.8 * cm, A4[1] - 1.35 * cm, A4[0] - 1.8 * cm, A4[1] - 1.35 * cm)
    canvas.setFont("Helvetica", 7)
    canvas.setFillColor(HexColor("#444444"))
    canvas.drawString(1.8 * cm, A4[1] - 1.1 * cm, "Deteccao de Fraudes com Lakehouse Local e Apache Spark")
    canvas.drawRightString(A4[0] - 1.8 * cm, 1.05 * cm, str(document.page))
    canvas.restoreState()


def main() -> None:
    raw = SOURCE.read_text(encoding="utf-8")
    title = re.search(r"\\title\{(.+?)\}", raw, flags=re.S).group(1)
    author = re.search(r"\\author\{(.+?)\}", raw, flags=re.S).group(1)
    body = raw.split("\\begin{document}", 1)[1].split("\\end{document}", 1)[0]
    styles = getSampleStyleSheet()
    normal = ParagraphStyle("Article", parent=styles["BodyText"], fontName="Helvetica", fontSize=9.2, leading=12.2, alignment=TA_JUSTIFY, spaceAfter=6)
    abstract = ParagraphStyle("Abstract", parent=normal, fontSize=8.6, leading=11.2, leftIndent=7, rightIndent=7)
    heading = ParagraphStyle("Heading", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=10.5, leading=12.5, spaceBefore=7, spaceAfter=4)
    subheading = ParagraphStyle("Subheading", parent=styles["Heading3"], fontName="Helvetica-Bold", fontSize=8.8, leading=10.5, spaceBefore=5, spaceAfter=3)
    title_style = ParagraphStyle("Title", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=15, leading=18, alignment=TA_CENTER, spaceAfter=6)
    byline = ParagraphStyle("Byline", parent=normal, alignment=TA_CENTER, spaceAfter=12)
    references = ParagraphStyle("References", parent=normal, fontSize=8.0, leading=10.0, leftIndent=6, firstLineIndent=-6)

    doc = BaseDocTemplate(str(OUTPUT), pagesize=A4, leftMargin=1.8 * cm, rightMargin=1.8 * cm, topMargin=1.7 * cm, bottomMargin=1.55 * cm)
    available = A4[0] - doc.leftMargin - doc.rightMargin
    gap = 0.55 * cm
    width = (available - gap) / 2
    height = A4[1] - doc.topMargin - doc.bottomMargin
    frames = [Frame(doc.leftMargin, doc.bottomMargin, width, height, id="left"), Frame(doc.leftMargin + width + gap, doc.bottomMargin, width, height, id="right")]
    doc.addPageTemplates([PageTemplate(id="twoColumns", frames=frames, onPage=page)])

    story = [Paragraph(escape(strip_latex(title)), title_style), Paragraph(escape(strip_latex(author) + " -- Junho de 2026"), byline)]
    chunks = re.split(r"\n\s*\n", body)
    in_references = False
    for chunk in chunks:
        chunk = chunk.strip()
        if not chunk or chunk in {"\\maketitle", "\\end{thebibliography}"}:
            continue
        if chunk.startswith("\\begin{abstract}"):
            text = strip_latex(chunk.replace("\\begin{abstract}", "").replace("\\end{abstract}", ""))
            story.extend([Paragraph("<b>Resumo.</b> " + escape(text), abstract), Spacer(1, 4)])
            continue
        section = re.match(r"\\section\{([^}]*)\}", chunk)
        if section:
            name = strip_latex(section.group(1))
            if name != "Introduction":
                story.append(PageBreak())
            in_references = name == "References"
            story.append(Paragraph(escape(name), heading))
            remainder = strip_latex(chunk[section.end():])
            if remainder:
                story.append(Paragraph(escape(remainder), normal))
            continue
        subsection = re.match(r"\\subsection\{([^}]*)\}", chunk)
        if subsection:
            subsection_name = strip_latex(subsection.group(1))
            if subsection_name in {
                "Teste cronologico isolado",
                "Scores e alertas do modelo de implantacao",
                "Desempenho da pipeline e qualidade",
            }:
                story.append(PageBreak())
            story.append(Paragraph(escape(subsection_name), subheading))
            remainder = strip_latex(chunk[subsection.end():])
            if remainder:
                story.append(Paragraph(escape(remainder), normal))
            continue
        text = strip_latex(chunk)
        if text:
            story.append(Paragraph(escape(text), references if in_references else normal))
            if "[Figura" in text:
                story.append(Spacer(1, 95))
    doc.build(story)


if __name__ == "__main__":
    main()
