while read l; do
	wget "$l"
done < urls.txt
