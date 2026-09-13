# NEWS8K - all-items 8-K event dataset (SEC submissions API)

Built 2026-09-13 12:43. Source: https://data.sec.gov/submissions/CIK##########.json (`filings.recent` + every archive page in `filings.files` with filingTo >= 2004-01-01). Forms kept: every form starting with `8-K` (8-K, 8-K/A, 8-K12B, 8-K12G3, 8-K15D5). Items kept: ALL. Filing dates >= 2004-01-01.

- CIKs crawled: **5213** of 5,213 universe CIKs (company_tickers.json intersect us_common_symbols.csv, symbols as of 2026-09 -> survivorship: delisted names absent). Failed CIKs (after 10 retries): 0. CIKs mapped to >1 universe ticker (share classes): 25 - one row per filing, `ticker` = SEC primary, `tickers_all` pipe-joined.
- Rows: **701,481** unique (cik, accession) filings (0 duplicates dropped). Date range 2004-01-02 .. 2026-09-11. Item 2.02 filings: 213,697 (earn dataset had 196,281 from the same source - should match closely).
- Forms: 8-K 683,837, 8-K/A 17,326, 8-K12B 224, 8-K12G3 64, 8-K12B/A 21, 8-K12G3/A 7, 8-K15D5 2
- Timing (acceptance in ET; pre <09:30, intra 09:30-16:00, post >=16:00): post 394,706, pre 171,936, intra 134,839. zoneinfo cross-check: 0 mismatches of 701481.
- **Event-date rule for the consumer:** the tradeable reaction is the first RTH session strictly after acceptance: `pre` -> same `et_date` open; `intra` -> same session (already trading); `post` -> next trading day's open. Filings accepted after 17:30 ET carry the NEXT business day as SEC `filing_date` - use `et_date` + `timing`, not `filing_date`, to align with bars.
- Old item numbering: 9,156 filings dated before 2004-08-23 use the pre-reform integer items; `items_new` maps them (1->5.01, 2->2.01, 3->1.03, 4->4.01, 5->8.01, 6->5.02, 7->9.01, 8->5.03, 9->7.01, 10->5.05, 11->5.04, 12->2.02); `items` is the raw SEC string.

## Item code -> event type (standard map)

| item | event type | filings |
|---|---|---:|
| 1.01 | entry into material definitive agreement | 97,626 |
| 1.02 | termination of material agreement | 8,777 |
| 1.03 | bankruptcy/receivership | 163 |
| 1.04 | (other/uncommon) | 170 |
| 1.05 | material cybersecurity incident | 63 |
| 13 | (other/uncommon) | 3 |
| 2.01 | completion of acquisition/disposition | 12,026 |
| 2.02 | results of operations (earnings) | 213,697 |
| 2.03 | creation of direct financial obligation (debt) | 34,778 |
| 2.04 | triggering events accelerating obligations | 895 |
| 2.05 | exit/disposal costs (restructuring) | 3,459 |
| 2.06 | material impairments | 1,413 |
| 3.01 | delisting notice / listing-rule noncompliance | 7,556 |
| 3.02 | unregistered sale of equity (dilution) | 19,661 |
| 3.03 | material modification of security-holder rights | 7,070 |
| 4.01 | change in accountant | 5,256 |
| 4.02 | non-reliance on prior financials (restatement) | 1,685 |
| 5.01 | change in control | 1,706 |
| 5.02 | director/officer departure or appointment (executive change) | 122,071 |
| 5.03 | charter/bylaw amendment; fiscal-year change | 23,324 |
| 5.04 | trading suspension under benefit plans | 588 |
| 5.05 | code-of-ethics amendment/waiver | 1,165 |
| 5.06 | (other/uncommon) | 538 |
| 5.07 | submission of matters to shareholder vote | 47,650 |
| 5.08 | shareholder director nominations | 744 |
| 6.03 | (other/uncommon) | 4 |
| 7 | (other/uncommon) | 3 |
| 7.01 | Regulation FD disclosure | 154,429 |
| 8.01 | other events | 166,692 |
| 9.01 | financial statements and exhibits | 543,795 |

## Counts by year (filings, distinct CIKs, and filings carrying each key item)

```
item  filings  ciks   2.02  1.01  1.02  2.01  2.03  2.05  2.06  3.02  4.02  5.02  5.07   7.01   8.01   9.01
year                                                                                                       
2004    15435  1514   5528  1349   107   432   304    79    38   106    32   792     0   3051   4871  10022
2005    20920  1585   6122  5660   461   460   880   167   102   370   183  2422     0   3245   4573  14967
2006    21475  1646   6281  5337   375   454   877   157    96   394   168  2654     0   3349   5043  16194
2007    22041  1734   6555  3051   273   510   883   167    99   420    91  4369     0   3474   5390  17000
2008    21934  1784   6831  2583   223   411   733   203   132   458    64  4559     0   3669   5298  16883
2009    21417  1809   6839  2675   249   275   745   222   122   530    62  4145     0   3663   5486  16638
2010    22503  1882   6952  2809   326   427   902   115    51   461    38  3830  1618   4140   5319  17123
2011    24188  1929   7155  3115   413   484  1154    96    54   411    40  4104  2180   4412   5745  18028
2012    25027  1988   7464  3084   329   486  1181   137    68   447    47  4295  1936   4827   6199  19144
2013    25777  2090   7781  3284   271   499  1270   123    63   555    37  4448  2002   5052   6225  19856
2014    27518  2221   8337  3472   304   546  1368    99    48   596    30  4869  2093   5686   6538  21204
2015    29394  2335   8773  3731   352   621  1496   151    56   633    47  5314  2214   6204   6996  22602
2016    29843  2439   9084  3817   349   524  1553   142    53   708    38  5229  2333   6389   7021  22919
2017    31574  2530   9463  4309   366   546  1799   118    38   925    30  5532  2704   6699   7460  24329
2018    32782  2669   9980  4318   429   595  1793   117    40   908    44  5923  2629   7410   7683  25246
2019    34156  2790  10400  4480   374   576  1857   134    57   964    26  6250  2731   7689   7777  26337
2020    39402  3028  11327  5530   458   517  2559   174    53  1226    44  6842  2913   9450  10195  30964
2021    41348  3364  12289  5952   575   766  2350    70    30  1420   216  7449  3165  10236  10178  33015
2022    40585  3468  13210  4806   461   564  2011   146    33   943    86  7733  3538  10206   8438  32241
2023    43204  3576  13588  5232   468   481  1993   254    41  1240   125  7825  3891  11085   9459  34310
2024    45108  3741  14019  6334   505   554  2410   236    49  1834   107  8012  3916  11592  10286  35977
2025    48826  4034  14510  7326   637   683  2710   212    62  2327    74  8784  4132  12949  11893  39175
2026    37024  4241  11209  5372   472   615  1950   140    28  1785    56  6691  3655   9952   8619  29621
```

## Item co-occurrence notes

Top item combinations (9.01 exhibits stripped):

| items (ex 9.01) | filings |
|---|---:|
| 2.02 | 159,992 |
| 8.01 | 107,315 |
| 7.01 | 79,902 |
| 5.02 | 78,725 |
| 2.02,7.01 | 31,405 |
| 5.07 | 30,521 |
| 1.01 | 29,861 |
| 1.01,2.03 | 15,739 |
| 2.02,8.01 | 9,754 |
| 5.02,7.01 | 9,553 |
| (9.01 only) | 8,309 |
| 7.01,8.01 | 7,786 |
| 1.01,8.01 | 7,719 |
| 5.03 | 7,422 |
| 5.02,5.07 | 6,558 |
| 5.02,8.01 | 5,918 |
| 1.01,7.01 | 5,126 |
| 3.01 | 4,960 |
| 4.01 | 4,412 |
| 2.01 | 3,544 |
| 1.01,3.02 | 3,334 |
| 1.01,2.03,8.01 | 3,011 |
| 1.01,5.02 | 2,941 |
| 3.02 | 2,546 |
| 2.02,7.01,8.01 | 2,525 |

## Files

- `research/news8k_events.csv` - columns: cik, ticker, tickers_all, accession, form, filing_date, report_date, acceptance_utc, acceptance_et, et_date, timing, items, items_new, old_numbering
- `research/news8k/sec_8k_all_raw_N.jsonl` - one line per CIK {cik, tickers, sec_tickers, name, sic, exch, narch, n, f:[{a,form,fd,rd,acc,it}]}
- `research/news8k/NEWS8K_sec_8k_all_fetch.py` (crawler, resumable, 4 shards x 0.5 s), `NEWS8K_flatten.py` (generator of this file), `shard_N.log`, `failed_N.txt`
## Coverage & cross-checks (crawl finished 2026-09-13 12:42, 4 shards x 0.5 s, 7,506 requests, 0 HTTP errors, 0 failed CIKs)

- 5,213/5,213 universe CIKs crawled; **829 CIKs have zero 8-K filings since 2004** (foreign private issuers that file 6-K instead - e.g. AEM - plus ETFs/trusts and 2026 listings), so **4,384 CIKs / 4,384 primary tickers** carry >=1 event. 2,293 archive pages were fetched on top of the 5,213 `recent` blocks.
- Distinct CIKs filing per year rises monotonically 1,514 (2004) -> 4,241 (2026): this is survivorship (symbol list as of 2026-09), not market growth. Any test spanning years must condition on that or use per-year cross-sections.
- **Containment check vs the July earnings set (`earn/sec_sub_raw_*.jsonl`, 196,281 Item-2.02 filings): all 196,281 accessions are present here (0 missing).** This set has 213,697 filings tagged 2.02 in `items_new`; the surplus comes from (a) pre-2004-08-23 filings whose old Item 12 is mapped to 2.02 here (the earn fetcher only matched the literal string `2.02`), and (b) the earn fetcher skipped archive pages with filingTo < 2014-01-01, so it under-collected 2004-2013 earnings 8-Ks for heavy filers. For pre-2014 earnings work prefer this file.
- One filing has a blank `items` string; all 701,481 rows have a parseable `acceptanceDateTime` (timing never `na`). ET conversion cross-checked against zoneinfo America/New_York: 0 mismatches.
- Not included (by construction): 6-K (foreign issuers), press releases that never became an 8-K, and the text of the filings themselves - `items` codes are the only content signal. Exhibit text (EX-99.1 press releases) would need a second crawl of the filing index per accession (~700k documents) - not attempted.
