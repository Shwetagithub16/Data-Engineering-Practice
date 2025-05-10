
#### Problems Statement

Our task is two use `Python` to find all the `json` files located in the `data` folder.
Once we find them all, read them with `Python` and convert them to `csv` files, to do this
we will have to flatten out some of the nested `json` data structures.

For example there is a `{"type":"Point","coordinates":[-99.9,16.88333]}` that must flattened.

Generally, our script should do the following ...
1. Crawl the `data` directory with `Python` and identify all the `json` files.
2. Load all the `json` files.
3. Flatten out the `json` data structure.
4. Write the results to a `csv` file, one for one with the json file, including the header names.
