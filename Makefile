compile:
	rm -rf html list_robots.txt list_sitemap.txt webcrawler.py output.txt && jupyter nbconvert --to python webcrawler.ipynb

run:
	python3 -W ignore webcrawler.py --num=10000