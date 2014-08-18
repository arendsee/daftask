daftask
=======

## Installation

```bash
./setup.sh
```

This script does the following:

 1. downloads the required data files
 2. writes them into an SQLite database
 3. prepares the following alias scripts:

    * ngi2taxid
    * pgi2taxid
    * sciname2taxid
    * taxid2ngi
    * taxid2pgi
    * taxid2sciname
 4. cleans up unneeded data files

Downloading and database preparation will require about one hour and about 14G
of harddrive space.

After running the setup script, copy the alias files into a folder in our path (e.g. usr/local/bin)

## Usage

I recommend using only the alias scripts rather than daftask itself.

Each of the alias files works in the same manner:

 * INPUT: a list of identifiers as arguments or from STDIN (one entry per line)
 * OUTPUT: a TAB delimited list of input and output identifiers
 * Note: The output order is determined by the internals of the SQL database, so is variable

## Examples

``` bash
$ taxid2sciname 284812 3702 9606
3702	Arabidopsis thaliana
9606	Homo sapiens
284812	Schizosaccharomyces pombe 972h-
```
