#!/usr/bin/perl -w

#Cohn lab script for adding unified locus [chromosome]:[location] column to Proton VCF outputs as well as
#splitting the info cell of each line into usable columns.  Also autogenerates the mySQL script required
#to read the data into a mySQL table.
#Copyright 2014 Michael Weinstein, Daniel H. Cohn laboratory, UCLA.

use strict;
use warnings;
use Getopt::Std;
$|=1;


sub main(){

    my %opts;  #setup the hash for commandline options
    getopts('f:', \%opts);  #write the commandline options to the array
    unless(checkopts(\%opts)){  #check the options for validity
        usage();  #if they are not valid (function returns false), print the usage instructions
    }
    my $vcfin = $opts{f};  #sets up the variable for the file to be annotated using the commandline option
    my $txtout = $vcfin."\.SQLready\.txt";  #sets up the variable for the output file for annotated data
    my $sqlout = $vcfin.".sql";  #sets up the variable for the sql script file
    open(VCF, $vcfin) or die "Couldn't open file $vcfin. \n"; #opens the input file or quits
    if (-e $txtout) {  #checks to see if the annotation output file already exists
        die "\nOutput file $txtout already appears to exist\.\n"; #quits if it does
    }
    if (-e $txtout."\.log") { #checks to see if the annotation log file already exists
        die "\nLog file $txtout\.log already appears to exist\.\n"; #quits if it does
    }
    if (-e $txtout."\.sql") { #checks to see if the SQL script file already exists
        die "\nLog file $txtout\.sql already appears to exist\.\n"; #quits if it does
    }
    
 

    open(OUTPUT, '>'.$txtout) or die "\nError opening output file $txtout\.\n";  #Creates the file for annotation output
    open(LOGFILE, '>'.$txtout."\.log") or die "\nError opening log file $txtout\.log\.\n"; #Creates the log file
   
    my @headers;  #an array of the header values to write them out in order
    my $headerindex;  
    my %names;  #a has to turn abbreviations into full names (or full names into SQL-friendly full names)
    my $progress; #counts the progress for output to user
    my $hardheaders; #counts the hard-written headers in the VCF
    my $headerscount;  #a count of all the possible headers
    my %columnposition;  #a hash to identify which column a given data field is in

    print "Finding existing headers\.\n";
    while (my $line = <VCF>){  #reads a line from the VCF
        chomp $line;  #eliminates trailing line breaks
        if ($line =~ /^\#\w/) { #asks if the line starts with a single hash tag and then a letter
            print "Found headers\!\n";
            my @line = split (/\t/, $line);  #splits the headerline on tabs into an array
            $hardheaders = scalar(@line); #counts the elements of the array (which is the number of hard-written headers)
            for (my $index = 0; $index <= $hardheaders-1; $index++){ #a loop to go through each of the existing headers
                $line[$index] =~ s/\%/\_percent/g; #replaces the percent symbol (forbidden in field identifiers) with the word
                $line[$index] =~ s/\W/\_/g; #replaces any disallowed characters in field identifiers with underscores
                $columnposition{$line[$index]} = $index;  #writes to the column position hash with the column name as the key and the location (indexed to zero to match with the array structure) as the value
                $names{$line[$index]} = $line[$index];  #writes the name of the column to both the key and value of the names array (these are not abbreviated and don't need a key to describe them)
            }
            @headers = @line;  #writes the cleaned up line to the headers array (this is OK because these are the first entries)
        }  
    }
    $columnposition{'uniLoc'} = $hardheaders;  #creates an entry in the columnposition hash for uniLoc with a value of the next free column
    $names{'uniLoc'} = "uniLoc";  #creates an entry for uniLoc in the names hash with uniLoc for both key and value
    $headers[$hardheaders] = "uniLoc"; #adds uniLoc to the array of headers
    $hardheaders++; #increments up the count of existing headers
    $columnposition{'lineNumber'} = $hardheaders; #the following lines do the same thing for a line number column
    $names{'lineNumber'} = "lineNumber";
    $headers[$hardheaders] = "lineNumber";
    $hardheaders++;
    
    seek VCF,0,0;  #resets VCF to starting position for the next steps 
    $headerindex = $hardheaders;  #initializes the header index to the hard-written headers variable
    $progress = 0; #initializes the progress counter to zero
    undef my $headerwritten;  #ensures that the variable monitoring if the headers have been written starts as false (undefined)
    my $datalinenumber = 1;  #counts the number of data lines
    
    LINE: while (my $line = <VCF>) { #starts a loop called line to read through lines of the VCF
        chomp $line; #takes the read line and eliminates any trailing linebreaks
        $progress++; #increments progress
        if ($line =~ /^\#/) { #checks if the line starts with a hash tag
            if ($line =~ /\#\#INFO\=\<ID\=(.+?)\,.*?Description\=\"(.+?)\"\>/i) {  #checks if the line starts with two hashtags followed by INFO (indicating that the line says what an abbreviated header means)
                my $abbreviation = $1;  #takes the first parenthetical value from the regex and saves it as abbreviation
                my $longname = $2; #takes the second parenthetical value and saves it as the long name of the value
                $longname =~ s/\%/\_percent/g; #replaces the percent symbol (forbidden in field identifiers) with the word
                $longname =~ s/\W/\_/g; #replaces any disallowed characters in field identifiers with underscores
                $names{$abbreviation} = $longname; #writes to the names hash using the abbreviation as the key and long name as the value
                $headers[$headerindex] = $longname; #writes the longname to the next spot in the header array
                $columnposition{$longname} = $headerindex; #writes the longname of the column to the position hash with the position as the key (again, indexed to 0)
                $headerindex++;  #increments the header index
                print "Processed $progress lines\.\r"; #reports progress
                next LINE; #moves on to the next line
            }
            else{  #if the hash tagged line is not an info line
                print LOGFILE "Skipped line $progress\; appears to be a comment\.\n"; #notes that it was skipped in the log
                print "Processed $progress lines\.\r"; #updates the progress report
                next LINE; #moves on to the next line
            }
        }
        
        unless ($headerwritten){  #checks if the variable for the header having been written is true (it will only reach this after finishing the hash-tagged lines)
            my $wholeheader;  #declares a variable for generating the whole header line
            foreach my $header(@headers){  #iterates through each of the header values
                if ($wholeheader) {
                    $wholeheader = $wholeheader.$header."\t";  #adds each header value to the growing string of headers, separated by a tab
                }
                else{
                    $wholeheader = $header."\t";  #writes the current header to the whole header (only done if no values are already there)
                }
                
                
            }
            $wholeheader =~ s/\t$/\n/;  #replaces the last tab on the header with a linebreak
            print OUTPUT $wholeheader; #and writes it to a file          
            $headerwritten = "TRUE"; #sets the variable for header being written to true (prevents repeating this loop for every line and writing the header over and over)
        }    
        
        if ($line =~ /^\w/){  #if the line starts with an alphaneumeric character (or a line break)
            my @line = split (/\t/, $line);  #splits the line into an array on each tab
            my $chromosome = $line[$columnposition{'_CHROM'}];  #sets the variable for chromosome equal to the array position listed under _CHROM in the position hash (most likely position 0)
            $chromosome =~ s/chr//i; #removes the chr before the actual chromosome name (if there is one present)
            my $uniLoc = $chromosome.":".$line[$columnposition{'POS'}]; #looks up the array position where the chromosomal position is kept and combines that with the chromosome to create the uniLoc value
            my $index = scalar(@line);  #counts how many values are already in the line array
            $line[$index] = $uniLoc; #writes the uniLoc value just created to the next array position
            $index++; #increments the index (counting array positions)
            $line[$index] = $datalinenumber; #writes the dataline number to the next position in the array
            $datalinenumber++; #increments the dataline number
            my @info = split(/\;/, $line[$columnposition{'INFO'}]); #splits the cell containing the info on each semicolon
            foreach my $item(@info){ #goes through each item in the info array we just generated
                my @item = split (/\=/, $item); #splits the item on the equal sign (abbreviated value name on the left of the equals, observed value on the right)
                $line[$columnposition{$names{$item[0]}}] = $item[1]; #looks up the long name based on the abbreviation and uses that to look up the array position for that value, then writes the value to that position of the line array
            }
            undef my $lineout; #makes sure that the lineout variable is cleared
            foreach my $datum(@line){ #iterates through each value in the line array (some may be undefined)
                if (defined $datum) {  #if the value is defined (something was written to it)
                    $lineout = $lineout.$datum; #adds the just-read value to the growing line of data
                }
                $lineout = $lineout."\t"; #adds a tab to separate the values (does this even if the datum was undefined)
            }
            $lineout =~ s/\t$/\n/; #takes the tab that was written at the very end of the lineout (which is now complete) and replaces it with a line break
            print OUTPUT $lineout; #and writes it to the file
            print "Processed $progress lines\.\r"; #updates the progress
        }
        else{
            print "Possible error at line $progress\.  Unsure if comment or data\.\n";  #if the line is not recognized as a comment or data, it gets reported to the user
            print LOGFILE "Possible error at line $progress\.  Unsure if comment or data\.\n";  #and also added to the logfile
            print "Processed $progress lines\.\r";  #Updates the progress
        }
    }
    
    close LOGFILE;  #closes the log file
    close OUTPUT;  #closes the output (it will be reopened briefly in the next section to read the headers for SQL code generation)
    
        print "\nDone generating the input file\, now generating the SQL script\!\n";  #prints done in the console
    
    
    my $dbname;  #declares a variable for database name
    my $tablename;  #declares a variable for table name
    while (!defined $dbname) {  #until the table name is defined
        print "In which database will we create this table\?\n";
        $dbname = readline STDIN;  #reads the user input line
        chomp $dbname;  #eliminates trailing characters and linebreaks from the input
        if ($dbname =~ /\W/) {  #checks the input for any non-word characters (anything other than alphaneumeric and underscore)
            print "Database name contains invalid characters\.  Valid characters are alphaneumeric or underscore\.\n";
            undef $dbname;  #undefines the database name before returning to the beginning of the loop
        }   
    }
    print "Please be sure that the database is already created before running the table creation script generated here\.\n";
    while (!defined $tablename) {  #this block of code does the same thing with the table name as we just did with the database name
        print "What do you wish to call the table\?\n";
        $tablename = readline STDIN;
        chomp $tablename;
        if ($tablename =~ /\W/) {
            print "Table name contains invalid characters\.  Valid characters are alphaneumeric or underscore\.\n";
            undef $tablename;
        }
    }

    unless (-e "protonpolisher.prefs.txt"){  #checks to see if the preferences file exists
        print "No field\-type preference file found\.  Generating default preference file\.\n";  #prints a message if not
        open (PREFS, ">protonpolisher.prefs.txt") or die "Unable to create preferences file.\n";  #creates a new preferences file using the two lines below
        print PREFS "_CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tuniLoc\tList_of_original_Hotspot_IDs\tList_of_original_allele_positions\tList_of_original_reference_bases\tList_of_original_variant_bases\tMaps_OID_OPOS_OREF_OALT_entries_to_specific_ALT_alleles\tAlternate_allele_observations\tTotal_read_depth_at_the_locus\tFlow_Evaluator_Alternate_allele_observations\tFlow_Evaluator_read_depth_at_the_locus\tReason_why_the_variant_was_filtered_\tFlow_Evaluator_Reference_allele_observations\tFlow_Evaluator_Alternate_allele_observations_on_the_forward_strand\tFlow_Evaluator_Alternate_allele_observations_on_the_reverse_strand\tFlow_Evaluator_Reference_observations_on_the_forward_strand\tFlow_Evaluator_Reference_observations_on_the_reverse_strand\tForward_strand_bias_in_prediction_\tFlow_Evaluator_failed_read_ratio\tRun_length__the_number_of_consecutive_repeats_of_the_alternate_allele_in_the_reference_genome\tIndicate_it_is_at_a_hot_spot\tallele_length\tMean_log_likelihood_delta_per_read_\tReason_why_the_variant_is_a_No_Call_\tNumber_of_samples_with_data\tQualityByDepth_as_4_QUAL_FDP__analogous_to_GATK_\tDistance_of_bias_parameters_from_zero_\tReference_Hypothesis_bias_in_prediction_\tReverse_strand_bias_in_prediction_\tReference_allele_observations\tAlternate_allele_observations_on_the_forward_strand\tAlternate_allele_observations_on_the_reverse_strand\tNumber_of_reference_observations_on_the_forward_strand\tNumber_of_reference_observations_on_the_reverse_strand\tStrand_specific_error_prediction_on_negative_strand_\tStrand_specific_error_prediction_on_positive_strand_\tStrand_specific_strand_bias_for_allele_\tStrand_bias_in_variant_relative_to_reference_\tThe_type_of_allele__either_snp__mnp__ins__del__or_complex_\tVariant_Hypothesis_bias_in_prediction_\tlineNumber\n";
        print PREFS "ENUM\(\'chr1\'\,\'chr2\'\,\'chr3\'\,\'chr4\'\,\'chr5\'\,\'chr6\'\,\'chr7\'\,\'chr8\'\,\'chr9\'\,\'chr10\'\,\'chr11\'\,\'chr12\'\,\'chr13\'\,\'chr14\'\,\'chr15\'\,\'chr16\'\,\'chr17\'\,\'chr18\'\,\'chr19\'\,\'chr20\'\,\'chr21\'\,\'chr22\'\,\'chrX\'\,\'chrY\'\,\'chrMT\'\) NOT NULL\tINT NOT NULL\tVARCHAR\(10\) NULL\tVARCHAR\(100\) NOT NULL\tVARCHAR\(255\) NULL\tFLOAT NOT NULL\tVARCHAR\(10\) NOT NULL\tTEXT\tVARCHAR\(255\) NULL\tVARCHAR\(15\) NOT NULL\tVARCHAR\(10\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(255\) NULL\tVARCHAR\(255\) NULL\tVARCHAR\(255\) NULL\tVARCHAR\(50\) NULL\tINT NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tFLOAT NULL\tVARCHAR\(50\) NULL\tVARCHAR\(10\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(100\) NULL\tVARCHAR\(10\) NULL\tVARCHAR\(10\) NULL\tFLOAT NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\)\tVARCHAR\(50\)\tVARCHAR\(50\)\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tINT NOT NULL";
        print "Field\-type preference file generated\, may be edited as a tab-delimited text if needed\.\n";
        close PREFS;  #closes the preferences file
    }
    
    open (PREFS, "protonpolisher.prefs.txt") or die "Unable to open preference file\.\n"; #opens the preferences file
    my $prefkey = <PREFS>;  #reads the first line of the file into prefkey
    my $prefvalue = <PREFS>;  #reads the second line of the file into prefvalue
    close PREFS;  #closes the file
    chomp $prefkey;  #eliminates leading or trailing spaces or linebreaks from this value
    chomp $prefvalue;  #same as above
    my @prefkey = split(/\t/, $prefkey);  #splits the prefkey string at each tab to create an array
    my @prefvalue = split(/\t/, $prefvalue);  #splits the prefvalue string at each tab to create an array
    my %prefhash = ();  #initializes an empty hash of preference values
    my $index = 0;  #initializes index
    my $fieldtype = 0;  #initializes the user-chosen field type value to 0
    foreach my $key(@prefkey){  #iterates through each entry in the prefkey array
        $prefhash{$prefkey[$index]} = $prefvalue[$index];  #writes each entry to the preferences hash for the corresponding key and value pairs
        $index ++;  #increments index
    }
    open(OUTPUT, $txtout) or die "\nError opening output file $txtout\.\n";  #reopens the file used to write the SQL-ready sequence
    my $headers = <OUTPUT>;  #reads the header (first) line from the file
    close OUTPUT;  #closes the file
    chomp $headers;  #eliminates any leading or trailing space or line break characters from the line
    @headers = split(/\t/, $headers);  #splits the string into an array
    foreach my $headervalue(@headers){  #goes through each header value in the headers
        if (!defined $prefhash{$headervalue}) {  #if it is undefined (becaue it was not in the preferences file)
            print "No default value found for $headervalue\.  Please select from one of the following\:\n";
            print "1  TEXT\n2  VARCHAR\(255\)\n3  INTEGER\n4  FLOAT\n";
            $fieldtype = readline STDIN;  #takes the user input
            chomp $fieldtype;  #eliminates leading and trailing linebreaks and other spacers
            while ($fieldtype !~ /^[1-4]$/) {  #keeps this loop going unless the fieldtype has been set to 1, 2, 3, or 4
                print "Invalid choice\, please select again\.\n";
                $fieldtype = readline STDIN;  #takes in another entry for the fieldtype
                chomp $fieldtype;
            }
            if ($fieldtype == 1) {  #if field type was one
                $prefhash{$headervalue} = "TEXT NULL";   #sets the matching preference hash value accordingly (next 3 statements do similar things)
            }
            if ($fieldtype == 2) {
                $prefhash{$headervalue} = "VARCHAR(255) NULL";
            }
            if ($fieldtype == 3) {
                $prefhash{$headervalue} = "INT NULL";
            }
            if ($fieldtype == 4) {
                $prefhash{$headervalue} = "FLOAT NULL";
            }
            
        }
        
    }
    print "Generating mySQL script\.\n";
    open(SQL, '>'.$txtout."\.sql") or die "\nError opening output file $txtout\.sql\.\n";  #opens the file that will contain the SQL script
    print SQL "USE $dbname \;\n";  #writes the first line of the file that says which database schema to use
    print SQL "CREATE TABLE $dbname\.$tablename \(\n";  #writes the next line to tell it to generate the table
    my $hlindex = 0;
    foreach my $headervalue(@headers) {  #iterates through the array of headers
        my $printheadervalue = $headervalue;
        if (length($printheadervalue) > 61) {  #if the length of the column name after adding VCF will be over 64 characters, mySQL will be very displeased with us      
            $printheadervalue =~ s/flow_evaluator/FlowEval/i;  #trimming to avoid column names over 64 characters
            $printheadervalue =~ s/Alternate/Alt/i;  #trimming to avoid column names over 64 characters
            $printheadervalue =~ s/Run_length__the_number_of_consecutive_repeats_of_the_alternate_allele_in_the_reference_genome/Run_len_number_of_consec_rpts_of_alt_allele_in_ref_genome/i; #This one needs special attention
            if (length($printheadervalue) > 61) {  #if that didn't get the job done
                $printheadervalue = substr($printheadervalue, 0, 61 ); #use brute force and just chop it down to 61 characters
            }
        }
        print SQL $printheadervalue."VCF ".$prefhash{$headervalue}."\,\n";  #putting out a line of code for each one to create the approprate field.  Ending names with VCF to avoid any possible collisions for common names like uniLoc, chromosome, position, etc.
        if ($headers[$hlindex+1]) {
            print SQL "\,\n";
        }
        else {
            print SQL "\)\;";
        }
        $hlindex ++;
    }
    print SQL "LOAD DATA LOCAL INFILE \'$txtout\' INTO TABLE $tablename\nCOLUMNS TERMINATED BY \'\\t\'\nLINES TERMINATED BY \'\\n\'\nIGNORE 1 lines\;";  #writes the final lines of the SQL script that tell it where to get the data and how to read it
    print SQL "ALTER TABLE $tablename ADD INDEX \(UnilocVCF\)\;\n";  #creates an index by locus
    close SQL;  #closes the SQL file
    print "Done\!\n";  #prints that it is done
}

sub checkopts{
    my $opts = shift;  #dereferences the hash containing the options
    
    my $file = $opts->{"f"}; #puts the value in options under key F into a variable called file
    
    unless(defined($file) and (-e $file)){  #unless the file entered exists...
        print "Input file not found or not defined in commandline arguments.\n";
        return 0;  #this function will return a value of 0, which is false and signals bad options
    }
}

sub usage{  #This subroutine prints directions
    print "This program will prepare a Proton VCF output for loading into mySQL by cleaning up the headers, adding a unified locus column, splitting the info cell of each line into new columns at the end of the line, and generating the script to load the charts\.\nPlease use full \(not relative\) filenames to ensure the SQL script executes properly\.\nSample commandline\:\nperl seattleSequel\.pl \-f file\.txt\.\n";
    die "";

}



main();



