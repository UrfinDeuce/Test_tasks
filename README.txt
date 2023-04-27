Test Task for Junior Python Developer (Intern):

We need to write a simple python script to synchronize two file directories. 
The script will get the directory names as input, let’s say source, and destination. 
And makes sure they’re in sync using the following logic:

●	If a file exists in the source but not in the destination, copy the file over.
●	If a file exists in the source, but has a different name than in the destination, rename the destination file to match.
●	If a file exists in the destination but not in the source, remove it.					

For the second criterion, you need to detect renames, you’ll have to inspect the content of files. 
For this, you can use a hashing function like MD5 or SHA-1.

Extra point: We want to be able to use the same script to sync directories over the cloud, or some storage systems other than a basic Unix filesystem, for this we need to have the correct set of abstractions over the filesystem that lets us switch to another filesystem storage API in future.
