divs = load '/pig/divs' as (exchange, symbol, date, dividends);
highdivs = stream divs through `/home/naga/bigdata/highdiv.pl` as (exchange, symbol, date, dividends);
dump highdivs;
