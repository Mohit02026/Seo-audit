"""
reporters.py — text/Markdown report generators.

  generate_summary(self)          — plain-text internal summary (bound to FullTechnicalAudit)
  generate_client_report(job)     — Markdown for client delivery
  generate_internal_report(job)   — Markdown technical deep-dive
"""
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Plain-text summary (method bound to FullTechnicalAudit)
# ---------------------------------------------------------------------------

def generate_summary(self) -> str:
    s = self.results.get("crawl_summary", {})
    robots_info = self.results.get("robots_txt", {})
    pagespeed = self.results.get("pagespeed_sample", [])
    sitemap_info = self.results.get("sitemap", {})
    audit_status = self.results.get("audit_status", "UNKNOWN")
    rc = self.results.get("redirect_checks", {})
    total = s.get("total_pages", 0)

    issues = []
    if s.get("no_meta_desc", 0):          issues.append("missing meta descriptions")
    if s.get("missing_titles", 0):         issues.append("missing page titles")
    if s.get("no_h1", 0):                  issues.append("missing H1 headings")
    if s.get("multi_h1", 0):               issues.append("multiple H1s")
    if s.get("status_4xx", 0) or s.get("status_5xx", 0): issues.append("error pages")
    if s.get("redirect_chains", 0):        issues.append("redirect chains")
    if s.get("pages_with_missing_alt", 0): issues.append("images missing alt text")
    if s.get("pages_without_og", 0):       issues.append("missing Open Graph tags")
    if s.get("pages_with_mixed_content", 0): issues.append("mixed content")
    if s.get("pages_not_https", 0):        issues.append("non-HTTPS pages")
    if s.get("thin_content_pages", 0):     issues.append("thin content pages")
    if s.get("orphan_pages_count", 0):     issues.append("orphan pages")
    if not s.get("pages_with_schema", 0):  issues.append("no structured data detected")
    if s.get("broken_external_links_count", 0): issues.append("broken external links")
    if s.get("urls_with_uppercase", 0) or s.get("urls_too_long", 0) or s.get("urls_with_underscores", 0):
        issues.append("URL hygiene issues")
    if rc.get("www_redirect", {}).get("status") == "warning":
        issues.append("www redirect misconfigured")
    if rc.get("http_to_https", {}).get("status") == "warning":
        issues.append("HTTP→HTTPS redirect missing")

    cwv_issues = list(set(
        (["slow loading (LCP)"] if any(i.get("lcp_ms", 0) > 4000 for i in pagespeed if "error" not in i) else []) +
        (["layout shifts (CLS)"] if any(i.get("cls", 0) > 0.25 for i in pagespeed if "error" not in i) else [])
    ))

    L: List[str] = []
    sep = "=" * 60
    L += [sep, "SEO TECHNICAL AUDIT SUMMARY", f"Website: {self.site_url}", f"Status: {audit_status}", sep, ""]

    L += ["Executive overview", "-" * 60]
    if audit_status == "CRAWL_BLOCKED_BY_ROBOTS":
        L.append("Homepage blocked by robots.txt.")
    elif total == 0:
        L.append("No pages crawled — site may block crawlers or be JS-only.")
    elif issues or cwv_issues:
        L.append(f"Crawled {total} pages. Issues: {', '.join(issues + cwv_issues)}.")
    else:
        L.append(f"Crawled {total} pages. No major issues detected.")
    L.append("")

    L += ["1. Robots.txt", "-" * 60,
          f"- Exists: {robots_info.get('exists')}",
          f"- Allows homepage: {robots_info.get('allows_homepage')}"]
    if robots_info.get("crawl_delay"):
        L.append(f"- Crawl-delay: {robots_info['crawl_delay']}s")
    if audit_status == "CRAWL_BLOCKED_BY_ROBOTS":
        L += ["Issue: Homepage blocked.", "", sep]
        return "\n".join(L)
    L.append("")

    L += ["2. Sitemap", "-" * 60]
    if sitemap_info.get("exists"):
        L += [f"- Found: {sitemap_info.get('url')}",
              f"- URLs listed: {sitemap_info.get('url_count', '?')}",
              f"- Is index: {sitemap_info.get('is_index', False)}"]
        gap = sitemap_info.get("url_count", 0) - total
        if gap > 5:
            L.append(f"Issue: Sitemap lists {sitemap_info['url_count']} URLs but only {total} crawled.")
    else:
        L += ["- No sitemap found.", "Issue: Add a sitemap.xml for better indexing."]
    L.append("")

    L += ["3. Crawl metrics", "-" * 60,
          f"- Pages: {total}",
          f"- 2xx: {s.get('status_2xx',0)}  3xx: {s.get('status_3xx',0)}  4xx: {s.get('status_4xx',0)}  5xx: {s.get('status_5xx',0)}",
          f"- Redirect chains: {s.get('redirect_chains',0)}",
          f"- Non-HTTPS: {s.get('pages_not_https',0)}",
          f"- Mixed content: {s.get('pages_with_mixed_content',0)}"]
    if s.get("redirect_chains", 0):      L.append("Issue: Multi-hop redirects detected.")
    if s.get("status_4xx", 0) or s.get("status_5xx", 0): L.append("Issue: Error pages found.")
    if s.get("pages_not_https", 0):      L.append("Issue: Pages served over HTTP.")
    if s.get("pages_with_mixed_content", 0): L.append("Issue: Mixed content on HTTPS pages.")
    L.append("")

    L += ["4. On-page SEO", "-" * 60,
          f"- Missing title: {s.get('missing_titles',0)}",
          f"- Title too long (>60): {s.get('long_titles',0)}",
          f"- Title too short (<30): {s.get('titles_too_short',0)}",
          f"- Missing meta desc: {s.get('no_meta_desc',0)}",
          f"- Meta desc too long (>160): {s.get('meta_desc_too_long',0)}",
          f"- Meta desc too short (<120): {s.get('meta_desc_too_short',0)}",
          f"- No H1: {s.get('no_h1',0)}",
          f"- Multiple H1s: {s.get('multi_h1',0)}",
          f"- Noindex pages: {s.get('noindex_pages',0)}"]
    for k, msg in [
        ("missing_titles", "Missing titles."),
        ("no_meta_desc", "Missing meta descriptions."),
        ("no_h1", "Pages without H1."),
        ("multi_h1", "Pages with multiple H1s."),
        ("long_titles", "Titles over 60 chars."),
        ("titles_too_short", "Titles under 30 chars."),
        ("meta_desc_too_long", "Meta descriptions over 160 chars."),
    ]:
        if s.get(k, 0): L.append(f"Issue: {msg}")
    L.append("")

    dups = self.results.get("duplicate_titles", {})
    dup_descs = self.results.get("duplicate_meta_descriptions", {})
    if dups or dup_descs:
        L += ["5. Duplicate content", "-" * 60]
        for t, c in list(dups.items())[:5]:
            L.append(f"  [{c}x] {t[:80]}")
        if dups: L.append("Issue: Duplicate titles.")
        for d, c in list(dup_descs.items())[:3]:
            L.append(f"  [{c}x] {d[:80]}")
        if dup_descs: L.append("Issue: Duplicate meta descriptions.")
        L.append("")

    if s.get("pages_with_missing_alt", 0):
        L += ["6. Images", "-" * 60,
              f"- Missing alt total: {s.get('images_missing_alt_total',0)}",
              f"- Pages affected: {s.get('pages_with_missing_alt',0)}"]
        for p in self.results.get("pages_missing_alt", [])[:5]:
            L.append(f"  {p['url'][:70]}  ({p['images_missing_alt']}/{p['images_total']} missing)")
        L += ["Issue: Images without alt text.", ""]

    broken = self.results.get("broken_internal_links", [])
    broken_ext = self.results.get("broken_external_links", [])
    if broken or broken_ext:
        L += ["7. Broken links", "-" * 60]
        if broken:
            L.append(f"Internal ({len(broken)}):")
            for b in broken[:10]:
                L.append(f"  {b.get('status_code')} → {b.get('url','')[:90]}")
            L.append("Issue: Broken internal links found.")
        if broken_ext:
            L.append(f"External ({len(broken_ext)}):")
            for b in broken_ext[:10]:
                L.append(f"  {b.get('status_code')} → {b.get('url','')[:90]}")
            L.append("Issue: Broken external links found.")
        L.append("")

    L += ["8. Social meta", "-" * 60,
          f"- Missing OG: {s.get('pages_without_og',0)} / {total}",
          f"- Missing Twitter Card: {s.get('pages_without_twitter',0)} / {total}"]
    if s.get("pages_without_og", 0): L.append("Issue: Pages missing OG tags.")
    if s.get("pages_without_twitter", 0): L.append("Issue: Pages missing Twitter Card.")
    L.append("")

    L += ["9. Structured data", "-" * 60,
          f"- Pages with schema: {s.get('pages_with_schema',0)} / {total}"]
    if not s.get("pages_with_schema", 0):
        L.append("Issue: No structured data detected — add JSON-LD schema.")
    L.append("")

    L += ["10. Content quality", "-" * 60,
          f"- Thin content (<300 words): {s.get('thin_content_pages',0)}",
          f"- Orphan pages (no inbound links): {s.get('orphan_pages_count',0)}"]
    if s.get("thin_content_pages", 0): L.append("Issue: Thin content pages found.")
    orphans = self.results.get("orphan_pages", [])
    if orphans:
        L.append("Issue: Orphan pages detected:")
        for u in orphans[:5]:
            L.append(f"  {u}")
    L.append("")

    url_issues = any([
        s.get("urls_with_uppercase", 0), s.get("urls_too_long", 0),
        s.get("urls_with_underscores", 0), s.get("urls_with_special_chars", 0),
        s.get("trailing_slash_issues_count", 0),
    ])
    if url_issues:
        L += ["10b. URL hygiene", "-" * 60]
        if s.get("urls_with_uppercase", 0):
            L.append(f"- URLs with uppercase letters: {s['urls_with_uppercase']}")
            L.append("Issue: Uppercase in URLs — use lowercase for consistency.")
        if s.get("urls_too_long", 0):
            L.append(f"- URLs over 115 chars: {s['urls_too_long']}")
            L.append("Issue: Overly long URLs — keep them short and descriptive.")
        if s.get("urls_with_underscores", 0):
            L.append(f"- URLs with underscores: {s['urls_with_underscores']}")
            L.append("Issue: Use hyphens instead of underscores in URLs.")
        if s.get("urls_with_special_chars", 0):
            L.append(f"- URLs with encoded special chars: {s['urls_with_special_chars']}")
            L.append("Issue: Special characters in URLs — clean up URL structure.")
        if s.get("trailing_slash_issues_count", 0):
            L.append(f"- Trailing slash inconsistencies: {s['trailing_slash_issues_count']}")
            L.append("Issue: Both /page and /page/ resolve — pick one and redirect the other.")
        L.append("")

    if rc:
        L += ["10c. Redirect setup", "-" * 60]
        www = rc.get("www_redirect", {})
        if www:
            st = www.get("status", "")
            if st == "ok":
                L.append(f"- www redirect: ✅ OK ({www.get('from')} → {www.get('redirects_to','')})")
            elif st == "warning":
                L.append(f"- www redirect: ⚠️  {www.get('detail','')}")
                L.append("Issue: www/non-www duplicate content risk — set up a permanent redirect.")
            else:
                L.append(f"- www redirect: {st} — {www.get('detail','')}")
        h2h = rc.get("http_to_https", {})
        if h2h and h2h.get("status") != "n/a":
            st = h2h.get("status", "")
            if st == "ok":
                L.append(f"- HTTP→HTTPS redirect: ✅ OK (→ {h2h.get('redirects_to','')})")
            elif st == "warning":
                L.append(f"- HTTP→HTTPS redirect: ⚠️  {h2h.get('detail','')}")
                L.append("Issue: HTTP not redirecting to HTTPS — users and bots may land on insecure version.")
            else:
                L.append(f"- HTTP→HTTPS: {st} — {h2h.get('detail','')}")
        L.append("")

    # Hreflang
    hreflang_count = s.get("pages_with_hreflang", 0)
    total = s.get("total_pages", 0)
    if hreflang_count:
        L += ["10d. Hreflang", "-" * 60,
              f"- Pages with hreflang tags: {hreflang_count} / {total}"]
        L.append("")

    # Canonical issues
    canonical_issues_count = s.get("canonical_issues_count", 0)
    if canonical_issues_count:
        L += ["10e. Canonical issues", "-" * 60,
              f"- Canonical conflicts detected: {canonical_issues_count}"]
        for ci in self.results.get("canonical_issues", [])[:5]:
            issue_type = ci.get("issue", "")
            if issue_type == "canonical_points_outside_crawl":
                L.append(f"  {ci['url'][:70]} → canonical not in crawl: {ci['canonical'][:70]}")
            else:
                L.append(f"  {ci['url'][:70]} → canonical mismatch: {ci['canonical'][:70]}")
        L.append("Issue: Canonical conflicts detected — review and fix canonical tags.")
        L.append("")

    L += ["11. Performance", "-" * 60,
          f"- Avg server response: {s.get('avg_response_time_ms',0)} ms",
          f"- Avg page size: {s.get('avg_page_size_kb',0)} KB",
          f"- Slow pages (>3s): {s.get('slow_server_response',0)}"]
    if s.get("slow_server_response", 0): L.append("Issue: Slow server responses.")
    if s.get("avg_page_size_kb", 0) > 500: L.append("Issue: Large average page size.")
    L.append("")

    if pagespeed:
        L += ["12. Core Web Vitals (mobile)", "-" * 60]
        for item in pagespeed:
            if "error" in item:
                L.append(f"  {item.get('url','')[:70]}: error")
                continue
            perf = item.get("performance_score")
            lcp  = item.get("lcp_ms")
            cls  = item.get("cls")
            fcp  = item.get("fcp_ms")
            tbt  = item.get("tbt_ms")
            L.append(f"  {item.get('url','')[:70]}")
            L.append(f"    Performance score: {perf}/100" if perf is not None else "    Performance score: N/A")
            L.append(f"    LCP: {lcp:.0f}ms" if lcp else "    LCP: N/A")
            L.append(f"    FCP: {fcp:.0f}ms" if fcp else "    FCP: N/A")
            L.append(f"    TBT: {tbt:.0f}ms" if tbt else "    TBT: N/A")
            L.append(f"    CLS: {cls:.3f}" if cls is not None else "    CLS: N/A")
        if cwv_issues: L.append("Issue: " + ", ".join(cwv_issues))
        L.append("")

    L.append(sep)
    return "\n".join(L)


# ---------------------------------------------------------------------------
# Client-facing Markdown report (standalone — used in main.py)
# ---------------------------------------------------------------------------

def generate_client_report(job: Dict[str, Any]) -> str:
    result = job.get("result_json") or {}
    url = job.get("url", "")
    lead = job.get("lead_name", "")
    created = job.get("created_at", "")[:10]

    crawl = result.get("crawl_summary", {})
    ps_data = result.get("pagespeed_sample", [])
    total_pages = crawl.get("total_pages", 1)

    lines: List[str] = []
    lines.append(f"# SEO Audit Report — {lead}")
    lines.append(f"**Site:** {url}  |  **Date:** {created}\n")
    lines.append("---\n")

    # 🔴 Critical Issues
    lines.append("## 🔴 Critical Issues\n")
    crits: List[str] = []

    if crawl.get("missing_titles", 0):
        crits.append(f"- **{crawl['missing_titles']} page(s) missing a title tag.** Every page needs a unique, descriptive title.")
    if crawl.get("no_meta_desc", 0):
        crits.append(f"- **{crawl['no_meta_desc']} page(s) missing a meta description.** These are shown in Google search results.")
    if crawl.get("no_h1", 0):
        crits.append(f"- **{crawl['no_h1']} page(s) have no H1 heading.** Search engines use H1s to understand page topics.")
    if crawl.get("status_4xx", 0) or crawl.get("status_5xx", 0):
        n = crawl.get("status_4xx", 0) + crawl.get("status_5xx", 0)
        crits.append(f"- **{n} broken page(s) found (4xx/5xx errors).** These harm user experience and crawlability.")
    if crawl.get("thin_content_pages", 0):
        crits.append(f"- **{crawl['thin_content_pages']} thin-content page(s)** (fewer than 300 words). Consider expanding or consolidating.")
    if crawl.get("pages_not_https", 0):
        crits.append(f"- **{crawl['pages_not_https']} page(s) not served over HTTPS.** Security is a confirmed Google ranking factor.")
    broken_ext = result.get("broken_external_links", [])
    if broken_ext:
        crits.append(f"- **{len(broken_ext)} broken external link(s) found.** Links pointing to dead pages hurt credibility.")
    if result.get("duplicate_titles"):
        crits.append(f"- **{len(result['duplicate_titles'])} duplicate title group(s) detected.** Each page needs a unique title.")
    if result.get("duplicate_meta_descriptions"):
        crits.append(f"- **{len(result['duplicate_meta_descriptions'])} duplicate meta description group(s).** Unique descriptions improve click-through rates.")

    lines.extend(crits) if crits else lines.append("✅ No critical issues found.")
    lines.append("")

    # 🟡 Warnings
    lines.append("## 🟡 Warnings\n")
    warns: List[str] = []

    if crawl.get("titles_too_short", 0):
        warns.append(f"- **{crawl['titles_too_short']} title(s) too short** (under 30 characters). Aim for 30–60 characters.")
    if crawl.get("meta_desc_too_long", 0):
        warns.append(f"- **{crawl['meta_desc_too_long']} meta description(s) too long** (over 160 characters). They will be truncated in search results.")
    if crawl.get("meta_desc_too_short", 0):
        warns.append(f"- **{crawl['meta_desc_too_short']} meta description(s) too short** (under 120 characters). More detail improves click-through rates.")
    if crawl.get("orphan_pages_count", 0):
        warns.append(f"- **{crawl['orphan_pages_count']} orphan page(s)** not linked from any other crawled page.")
    schema_pct = round(crawl.get("pages_with_schema", 0) / max(total_pages, 1) * 100)
    if schema_pct < 30:
        warns.append(f"- **Only {schema_pct}% of pages have structured data (Schema.org).** Adding JSON-LD can unlock rich results in Google.")
    sitemap = result.get("sitemap") or {}
    if not sitemap.get("exists"):
        warns.append("- **No XML sitemap found.** A sitemap helps search engines discover and index your pages.")
    robots_info = result.get("robots_txt", {})
    if not robots_info.get("exists"):
        warns.append("- **No robots.txt file detected.** This file guides search engine crawlers.")
    rc = result.get("redirect_checks", {})
    if rc.get("www_redirect", {}).get("status") == "warning":
        warns.append(f"- **www/non-www duplicate content risk.** {rc['www_redirect'].get('detail','Both versions resolve — set up a permanent redirect.')}")
    if rc.get("http_to_https", {}).get("status") == "warning":
        warns.append(f"- **HTTP not redirecting to HTTPS.** {rc['http_to_https'].get('detail','Set up a 301 redirect.')}")
    trailing = result.get("trailing_slash_issues", [])
    if trailing:
        warns.append(f"- **{len(trailing)} trailing slash inconsistenc{'y' if len(trailing)==1 else 'ies'} detected.** Pick one canonical form and redirect the other.")
    if crawl.get("urls_with_uppercase", 0):
        warns.append(f"- **{crawl['urls_with_uppercase']} URL(s) contain uppercase letters.** URLs should be lowercase.")
    if crawl.get("urls_too_long", 0):
        warns.append(f"- **{crawl['urls_too_long']} URL(s) are over 115 characters long.** Keep URLs short and descriptive.")
    if crawl.get("urls_with_underscores", 0):
        warns.append(f"- **{crawl['urls_with_underscores']} URL(s) use underscores.** Google recommends hyphens instead.")
    if crawl.get("canonical_issues_count", 0):
        warns.append(f"- **{crawl['canonical_issues_count']} canonical conflict(s) detected.** Pages with canonical tags pointing outside the crawled site or mismatching their final URL.")

    lines.extend(warns) if warns else lines.append("✅ No warnings.")
    lines.append("")

    # ✅ What's Working Well
    lines.append("## ✅ What's Working Well\n")
    goods: List[str] = []

    if not crawl.get("pages_not_https", 0) and total_pages:
        goods.append("- All pages are served over HTTPS.")
    if robots_info.get("exists"):
        goods.append("- A robots.txt file is present.")
    if sitemap.get("exists"):
        goods.append("- An XML sitemap was found and parsed successfully.")
    if crawl.get("pages_with_schema", 0):
        goods.append(f"- {crawl['pages_with_schema']} page(s) use structured data (Schema.org).")
    if not crawl.get("missing_titles", 0):
        goods.append("- All crawled pages have a title tag.")
    if not crawl.get("no_h1", 0):
        goods.append("- All crawled pages have an H1 heading.")
    if not crawl.get("status_4xx", 0) and not crawl.get("status_5xx", 0):
        goods.append("- No broken pages (4xx/5xx) detected.")
    if rc.get("www_redirect", {}).get("status") == "ok":
        goods.append("- www/non-www redirect is correctly configured.")
    if rc.get("http_to_https", {}).get("status") == "ok":
        goods.append("- HTTP redirects to HTTPS correctly.")

    lines.extend(goods) if goods else lines.append("_No standout positives yet — lots of room to improve!_")
    lines.append("")

    # ⚡ PageSpeed
    if ps_data:
        lines.append("## ⚡ Page Speed Snapshot\n")
        for ps in ps_data[:3]:
            perf = ps.get("performance_score")
            lcp  = ps.get("lcp_ms")
            fcp  = ps.get("fcp_ms")
            tbt  = ps.get("tbt_ms")
            cls  = ps.get("cls")
            lines.append(f"**{ps.get('url', '')}**")
            lines.append(f"- Performance score: {perf}/100" if perf is not None else "- Performance score: N/A")
            lines.append(f"- LCP: {lcp:.0f}ms" if lcp else "- LCP: N/A")
            lines.append(f"- FCP: {fcp:.0f}ms" if fcp else "- FCP: N/A")
            lines.append(f"- TBT: {tbt:.0f}ms" if tbt else "- TBT: N/A")
            lines.append(f"- CLS: {cls:.3f}" if cls is not None else "- CLS: N/A")
            lines.append("")

    # 📊 Crawl Overview
    lines.append("## 📊 Crawl Overview\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    lines.append(f"| Pages crawled | {crawl.get('total_pages', 0)} |")
    lines.append(f"| Pages with schema | {crawl.get('pages_with_schema', 0)} |")
    lines.append(f"| Images missing alt text | {crawl.get('images_missing_alt_total', 0)} |")
    lines.append(f"| Orphan pages | {crawl.get('orphan_pages_count', 0)} |")
    lines.append(f"| Broken external links | {crawl.get('broken_external_links_count', 0)} |")
    lines.append("")
    lines.append("---")
    lines.append("_Report generated by SEO Audit Tool_")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Internal technical Markdown report (standalone — used in main.py)
# ---------------------------------------------------------------------------

def generate_internal_report(job: Dict[str, Any]) -> str:
    result = job.get("result_json") or {}
    pages_data = job.get("pages_data_json") or []
    url = job.get("url", "")
    lead = job.get("lead_name", "")
    created = job.get("created_at", "")[:10]

    crawl = result.get("crawl_summary", {})
    robots = result.get("robots_txt", {})
    sitemap = result.get("sitemap")
    ps_data = result.get("pagespeed_sample", [])
    orphan_pages = result.get("orphan_pages", [])

    lines: List[str] = []
    lines.append(f"# Internal Technical SEO Audit — {lead}")
    lines.append(f"**Site:** {url}  |  **Date:** {created}  |  **Job:** {job.get('job_id', '')}")
    lines.append(f"**JS Enrichment:** {'Yes' if job.get('js_enrichment_used') else 'No'}")
    lines.append("\n---\n")

    lines.append("## Crawl Summary\n")
    lines.append("| Metric | Value |")
    lines.append("|--------|-------|")
    for k, v in crawl.items():
        lines.append(f"| {k.replace('_', ' ').title()} | {v} |")
    lines.append("")

    lines.append("## Robots.txt\n")
    lines.append(f"- Found: {robots.get('exists', False)}")
    lines.append(f"- Sitemap declared: {robots.get('sitemap_url', 'None')}")
    lines.append(f"- Crawl delay: {robots.get('crawl_delay', 'None')}")
    lines.append("")

    lines.append("## Sitemap\n")
    if sitemap and sitemap.get("exists"):
        lines.append(f"- URL: {sitemap.get('url', 'N/A')}")
        lines.append(f"- URLs found: {sitemap.get('url_count', 0)}")
    else:
        lines.append("- No sitemap found across checked paths.")
    lines.append("")

    rc = result.get("redirect_checks", {})
    if rc:
        lines.append("## Redirect Setup\n")
        www_r = rc.get("www_redirect", {})
        if www_r:
            icon = "✅" if www_r.get("status") == "ok" else "⚠️"
            lines.append(f"**www/non-www:** {icon} {www_r.get('status','')} — {www_r.get('detail') or www_r.get('redirects_to','')}")
        h2h = rc.get("http_to_https", {})
        if h2h and h2h.get("status") != "n/a":
            icon = "✅" if h2h.get("status") == "ok" else "⚠️"
            lines.append(f"**HTTP→HTTPS:** {icon} {h2h.get('status','')} — {h2h.get('detail') or h2h.get('redirects_to','')}")
        trailing = result.get("trailing_slash_issues", [])
        if trailing:
            lines.append(f"**Trailing slash issues:** {len(trailing)}")
            for t in trailing[:5]:
                lines.append(f"- {t.get('requested')} → {t.get('final')}")
        lines.append("")

    if ps_data:
        lines.append("## PageSpeed Insights\n")
        for ps in ps_data:
            lines.append(f"### {ps.get('url', '')}")
            lines.append("| Metric | Value |")
            lines.append("|--------|-------|")
            for m in ["lcp_ms", "inp_ms", "cls", "cwv_category"]:
                lines.append(f"| {m.replace('_', ' ').title()} | {ps.get(m, 'N/A')} |")
            lines.append("")

    broken_ext = result.get("broken_external_links", [])
    if broken_ext:
        lines.append("## Broken External Links\n")
        lines.append("| Status | URL |")
        lines.append("|--------|-----|")
        for b in broken_ext[:30]:
            lines.append(f"| {b.get('status_code')} | {b.get('url','')[:100]} |")
        lines.append("")

    url_issues = {k: crawl.get(k, 0) for k in [
        "urls_with_uppercase", "urls_too_long", "urls_with_underscores", "urls_with_special_chars"
    ]}
    if any(url_issues.values()):
        lines.append("## URL Hygiene\n")
        lines.append("| Issue | Count |")
        lines.append("|-------|-------|")
        labels = {
            "urls_with_uppercase": "URLs with uppercase letters",
            "urls_too_long": "URLs over 115 chars",
            "urls_with_underscores": "URLs with underscores (use hyphens)",
            "urls_with_special_chars": "URLs with encoded special chars",
        }
        for k, label in labels.items():
            if url_issues[k]:
                lines.append(f"| {label} | {url_issues[k]} |")
        lines.append("")

    if orphan_pages:
        lines.append("## Orphan Pages\n")
        for op in orphan_pages[:50]:
            lines.append(f"- {op}")
        if len(orphan_pages) > 50:
            lines.append(f"_...and {len(orphan_pages) - 50} more_")
        lines.append("")

    lines.append("## Per-Page Detail\n")
    col_headers = [
        "URL", "Status", "Depth", "Title", "Title Len",
        "H1", "Words", "Meta Desc", "Has Schema",
        "Schema Types", "Has OG", "Canonical", "Int Links", "Ext Links", "Used JS",
    ]
    lines.append("| " + " | ".join(col_headers) + " |")
    lines.append("| " + " | ".join(["---"] * len(col_headers)) + " |")

    for p in pages_data:
        def _s(val, maxlen=40):
            s = str(val) if val is not None else ""
            return s[:maxlen].replace("|", "\\|").replace("\n", " ")

        row = [
            _s(p.get("final_url") or p.get("url"), 60),
            _s(p.get("status_code")),
            _s(p.get("link_depth", 0)),
            _s(p.get("title"), 40),
            _s(p.get("title_length", "")),
            _s(p.get("h1_sample", ""), 30),
            _s(p.get("word_count", 0)),
            _s(p.get("has_meta_desc")),
            _s(p.get("has_schema")),
            _s(", ".join(p.get("schema_types", [])) if p.get("schema_types") else "", 30),
            _s(p.get("has_og")),
            _s(p.get("canonical"), 40),
            _s(p.get("internal_links_count", 0)),
            _s(p.get("external_links_count", 0)),
            _s(p.get("used_js_render", False)),
        ]
        lines.append("| " + " | ".join(row) + " |")

    lines.append("")
    lines.append("---")
    lines.append("_Internal use only — SEO Audit Tool_")
    return "\n".join(lines)
