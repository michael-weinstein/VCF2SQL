#!/usr/bin/perl -w

#Cohn lab script for adding unified locus [chromosome]:[location] column to VCF outputs as well as
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
    $columnposition{'A_read'} = $hardheaders;  
    $names{'A_read'} = "A_read";  
    $headers[$hardheaders] = "A_read"; 
    $hardheaders++;
    $columnposition{'T_read'} = $hardheaders;  
    $names{'T_read'} = "T_read";  
    $headers[$hardheaders] = "T_read"; 
    $hardheaders++;
    $columnposition{'C_read'} = $hardheaders;  
    $names{'C_read'} = "C_read";  
    $headers[$hardheaders] = "C_read"; 
    $hardheaders++;
    $columnposition{'G_read'} = $hardheaders;  
    $names{'G_read'} = "G_read";  
    $headers[$hardheaders] = "G_read"; 
    $hardheaders++;
    $columnposition{'A_percent'} = $hardheaders;  
    $names{'A_percent'} = "A_percent";  
    $headers[$hardheaders] = "A_percent"; 
    $hardheaders++;
    $columnposition{'T_percent'} = $hardheaders;  
    $names{'T_percent'} = "T_percent";  
    $headers[$hardheaders] = "T_percent"; 
    $hardheaders++;
    $columnposition{'C_percent'} = $hardheaders;  
    $names{'C_percent'} = "C_percent";  
    $headers[$hardheaders] = "C_percent"; 
    $hardheaders++;
    $columnposition{'G_percent'} = $hardheaders;  
    $names{'G_percent'} = "G_percent";  
    $headers[$hardheaders] = "G_percent"; 
    $hardheaders++;
    $columnposition{'Depth_Counted'} = $hardheaders;  
    $names{'Depth_Counted'} = "Depth_Counted";  
    $headers[$hardheaders] = "Depth_Counted"; 
    $hardheaders++;
    $columnposition{'Reference_reads'} = $hardheaders;  
    $names{'Reference_reads'} = "Reference_reads";  
    $headers[$hardheaders] = "Reference_reads"; 
    $hardheaders++;
    $columnposition{'Alt_reads'} = $hardheaders;  
    $names{'Alt_reads'} = "Alt_reads";  
    $headers[$hardheaders] = "Alt_reads"; 
    $hardheaders++;
    $columnposition{'Ref_read_percent'} = $hardheaders;  
    $names{'Ref_read_percent'} = "Ref_read_percent";  
    $headers[$hardheaders] = "Ref_read_percent"; 
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
            if ($line =~ /\#\#INFO\=\<ID\=(.+?)\,.*?Description\=\"(.+?)\"\>/i or $line =~ /\#\#FORMAT\=\<ID\=(.+?)\,.*?Description\=\"(.+?)\"\>/i) {  #checks if the line starts with two hashtags followed by INFO (indicating that the line says what an abbreviated header means)
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
            for (my $i = 0; $i < scalar(@headers); $i++){
                $headers[$i] =~ s/^_+//;
                $headers[$i] =~ s/_+$//;
                $headers[$i] =~ s/___/_/;
                $headers[$i] =~ s/__/_/;
            }
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
            $line[$columnposition{'_CHROM'}] =~ s/chr//i; #removes the chr before the actual chromosome name (if there is one present)
            my $chromosome = $line[$columnposition{'_CHROM'}];  #sets the variable for chromosome equal to the array position listed under _CHROM in the position hash (most likely position 0)
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
            my @format = split(/\:/,$line[$columnposition{'FORMAT'}]);  #takes the format cell from the line and creates an array from it
            my @rundata = split(/\:/, $line[$columnposition{'FORMAT'} + 1]); #does the same thing with the next column that should have the corresponding values for the variables named in the format array
            my $formatitems = scalar(@format);  #count the number of items we have in the format array (should be the same as with the variables from the next column over)
            for (my $i=0; $i<$formatitems; $i++){  #iterate through the format array using an index to compare cells between format and the next column with the values
                $line[$columnposition{$names{$format[$i]}}] = $rundata[$i];  #similar to above, looks up the variable abbreviation in the format array, gets the long name from the name hash, looks up the long name in the position hash, the writes the actual variable value to the indicated position in the line array.
            }
           
            if ($line[$columnposition{'FORMAT'} + 1] !~ /^\.\/\./ and $line[$columnposition{'Allelic_depths_for_the_ref_and_alt_alleles_in_the_order_listed'}]) {  #checks for null reads
                my %basehash = ("A",'0',"T",'0',"G",'0',"C",'0');  #initializes all the values in a hash of base => read values to 0
                my $totalreads = 0;  #initializes the count of reads to 0
                my @reads = split (/,/, $line[$columnposition{'Allelic_depths_for_the_ref_and_alt_alleles_in_the_order_listed'}]);  #creates an array of reads for each allele seen
                my @bases = split (/,/, $line[3]."\,".$line[4]); #creates an array of bases observed
                for (my $p = 0; $p < scalar(@reads); $p++){ #iterates through the arrays of reads and bases
                    $basehash{$bases[$p]} = $reads[$p];  #writes the number of reads to the base hash (it will create new values if the variant is not an SNV, but we can deal with that later)
                    $totalreads += $reads[$p];  #totals up the number of reads at the locus while we iterate through the loop
                }
                my $issnv = 'true';  #initializes the test of issnv (as in is SNV) to true
                my $wasread = 'true';  #initializes the test value of wasread to true
                foreach my $base(@bases){  #iterates through the reference and alternate alleles (the components of @bases)
                    if (length($base) > 1){  #if any of the bases were multiple characters (indicating a multi-nucleotide variant)
                        undef $issnv  #flag this row as not being SNV
                    }
                    if ($base eq 'N') {  #makes sure that the VCF doesn't have any N values that would pass the previous filter, but were not reads
                        undef $wasread  #mark it as a non-read (this would have most likely been caught already by the regex that starts the if statement about 15 lines above)
                    }
                    
                }
                if ($issnv and $wasread and $totalreads) {  #if the line was read as an SNV, the following lines add data to the line array at a position based on the column name
                    $line[$columnposition{'A_read'}] = $basehash{A};
                    $line[$columnposition{'A_percent'}] = $basehash{A}/$totalreads;
                    $line[$columnposition{'T_read'}] = $basehash{T};
                    $line[$columnposition{'T_percent'}] = $basehash{T}/$totalreads;
                    $line[$columnposition{'C_read'}] = $basehash{C};
                    $line[$columnposition{'C_percent'}] = $basehash{C}/$totalreads;
                    $line[$columnposition{'G_read'}] = $basehash{G};
                    $line[$columnposition{'G_percent'}] = $basehash{G}/$totalreads;
                    $line[$columnposition{'Depth_Counted'}] = $totalreads;
                    $line[$columnposition{'Reference_reads'}] = $basehash{$line[3]};
                    $line[$columnposition{'Alt_reads'}] = $totalreads - $basehash{$line[3]};
                    $line[$columnposition{'Ref_read_percent'}] = $basehash{$line[3]}/$totalreads;
                }
                else{
                    if ($wasread and $totalreads) {  #if the line was not read as an SNV, we only output alternate vs. reference allele read data (we can't cover every possible MNV)
                        $line[$columnposition{'Depth_Counted'}] = $totalreads;
                        $line[$columnposition{'Reference_reads'}] = $reads[0];
                        $line[$columnposition{'Alt_reads'}] = $totalreads - $reads[0];
                        $line[$columnposition{'Ref_read_percent'}] = $reads[0]/$totalreads;
                        
                    }
                }
            }
            else{   #if the line had genotype ./.  or a total read depth of 0 (or both, most likely), we will output all the read data as zeros.  We will skip the calculations above because there is a risk of trying to divide by zero.  Only Chuck Norris can divide by zero.
                $line[$columnposition{'Depth_Counted'}] = 0;
                $line[$columnposition{'Reference_reads'}] = 0;
                $line[$columnposition{'Alt_reads'}] = 0;
                $line[$columnposition{'Ref_read_percent'}] = 0;
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
    $tablename = $tablename.'VCF';    
    unless (-e "VCF2SQL.prefs.txt"){  #checks to see if the preferences file exists
        print "No field\-type preference file found\.  Generating default preference file\.\n";  #prints a message if not
        open (PREFS, ">VCF2SQL.prefs.txt") or die "Unable to create preferences file.\n";  #creates a new preferences file using the two lines below
        # The next two lines provide initialization values for the preferences file.  Don't mess with them, it's easier to edit the actual prefs file as a tab delimited text
        print PREFS "CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\tFORMAT\tuniLoc\tList_of_original_Hotspot_IDs\tList_of_original_allele_positions\tList_of_original_reference_bases\tList_of_original_variant_bases\tMaps_OID_OPOS_OREF_OALT_entries_to_specific_ALT_alleles\tAlternate_allele_observations\tTotal_read_depth_at_the_locus\tFlow_Evaluator_Alternate_allele_observations\tFlow_Evaluator_read_depth_at_the_locus\tReason_why_the_variant_was_filtered_\tFlow_Evaluator_Reference_allele_observations\tFlow_Evaluator_Alternate_allele_observations_on_the_forward_strand\tFlow_Evaluator_Alternate_allele_observations_on_the_reverse_strand\tFlow_Evaluator_Reference_observations_on_the_forward_strand\tFlow_Evaluator_Reference_observations_on_the_reverse_strand\tForward_strand_bias_in_prediction_\tFlow_Evaluator_failed_read_ratio\tRun_length__the_number_of_consecutive_repeats_of_the_alternate_allele_in_the_reference_genome\tIndicate_it_is_at_a_hot_spot\tallele_length\tMean_log_likelihood_delta_per_read_\tReason_why_the_variant_is_a_No_Call_\tNumber_of_samples_with_data\tQualityByDepth_as_4_QUAL_FDP__analogous_to_GATK_\tDistance_of_bias_parameters_from_zero_\tReference_Hypothesis_bias_in_prediction_\tReverse_strand_bias_in_prediction_\tReference_allele_observations\tAlternate_allele_observations_on_the_forward_strand\tAlternate_allele_observations_on_the_reverse_strand\tNumber_of_reference_observations_on_the_forward_strand\tNumber_of_reference_observations_on_the_reverse_strand\tStrand_specific_error_prediction_on_negative_strand_\tStrand_specific_error_prediction_on_positive_strand_\tStrand_specific_strand_bias_for_allele_\tStrand_bias_in_variant_relative_to_reference_\tThe_type_of_allele__either_snp__mnp__ins__del__or_complex_\tVariant_Hypothesis_bias_in_prediction_\tlineNumber\tAllelic_depths_for_the_ref_and_alt_alleles_in_the_order_listed\tApproximate_read_depth_reads_with_MQ_255_or_with_bad_mates_are_filtered\tGenotype_Quality\tGenotype\tNormalized_Phred_scaled_likelihoods_for_genotypes_as_defined_in_the_VCF_specification\tAllele_count_in_genotypes_for_each_ALT_allele__in_the_same_order_as_listed\tAllele_Frequency_for_each_ALT_allele__in_the_same_order_as_listed\tTotal_number_of_alleles_in_called_genotypes\tZ_score_from_Wilcoxon_rank_sum_test_of_Alt_Vs_Ref_base_qualities\tdbSNP_Membership\tApproximate_read_depth_some_reads_may_have_been_filtered\tWere_any_of_the_samples_downsampled\tFraction_of_Reads_Containing_Spanning_Deletions\tStop_position_of_the_interval\tPhred_scaled_p_value_using_Fisher_s_exact_test_to_detect_strand_bias\tConsistency_of_the_site_with_at_most_two_segregating_haplotypes\tInbreeding_coefficient_as_estimated_from_the_genotype_likelihoods_per_sample_when_compared_against_the_Hardy_Weinberg_expectation\tMaximum_likelihood_expectation_MLE__for_the_allele_counts__not_necessarily_the_same_as_the_AC_for_each_ALT_allele__in_the_same_order_as_listed\tMaximum_likelihood_expectation_MLE__for_the_allele_frequency__not_necessarily_the_same_as_the_AF_for_each_ALT_allele__in_the_same_order_as_listed\tRMS_Mapping_Quality\tTotal_Mapping_Quality_Zero_Reads\tZ_score_From_Wilcoxon_rank_sum_test_of_Alt_vs_Ref_read_mapping_qualities\tThis_variant_was_used_to_build_the_negative_training_set_of_bad_variants\tThis_variant_was_used_to_build_the_positive_training_set_of_good_variants\tVariant_Confidence_Quality_by_Depth\tNumber_of_times_tandem_repeat_unit_is_repeated_for_each_allele__including_reference\tTandem_repeat_unit_bases\tZ_score_from_Wilcoxon_rank_sum_test_of_Alt_vs_Ref_read_position_bias\tVariant_is_a_short_tandem_repeat\tLog_odds_ratio_of_being_a_true_variant_versus_being_false_under_the_trained_gaussian_mixture_model\tThe_annotation_which_was_the_worst_performing_in_the_Gaussian_mixture_model_likely_the_reason_why_the_variant_was_filtered_out\tA_read\tA_percent\tT_read\tT_percent\tC_read\tC_percent\tG_read\tG_percent\tDepth_Counted\tReference_reads\tAlt_reads\tRef_read_percent\n";
        print PREFS "ENUM\(\'1\'\,\'2\'\,\'3\'\,\'4\'\,\'5\'\,\'6\'\,\'7\'\,\'8\'\,\'9\'\,\'10\'\,\'11\'\,\'12\'\,\'13\'\,\'14\'\,\'15\'\,\'16\'\,\'17\'\,\'18\'\,\'19\'\,\'20\'\,\'21\'\,\'22\'\,\'X\'\,\'Y\'\,\'MT\'\) NOT NULL\tINT NOT NULL\tVARCHAR\(10\) NULL\tVARCHAR\(100\) NOT NULL\tVARCHAR\(255\) NULL\tFLOAT NOT NULL\tVARCHAR\(10\) NOT NULL\tTEXT\tVARCHAR\(255\) NULL\tVARCHAR\(15\) NOT NULL\tVARCHAR\(10\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(255\) NULL\tVARCHAR\(255\) NULL\tVARCHAR\(255\) NULL\tVARCHAR\(50\) NULL\tINT NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tFLOAT NULL\tVARCHAR\(50\) NULL\tVARCHAR\(10\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(100\) NULL\tVARCHAR\(10\) NULL\tVARCHAR\(10\) NULL\tFLOAT NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\)\tVARCHAR\(50\)\tVARCHAR\(50\)\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tVARCHAR\(50\) NULL\tINT NOT NULL\tVARCHAR\(100\)\tVARCHAR\(100\)\tINT\tVARCHAR\(10\)\tVARCHAR\(100\)\tVARCHAR\(100\)\tVARCHAR\(100\)\tINT\tFLOAT\tVARCHAR\(100\)\tINT\tVARCHAR\(100\)\tFLOAT\tVARCHAR\(100\)\tFLOAT\tFLOAT\tVARCHAR\(100\)\tVARCHAR\(100\)\tVARCHAR\(100\)\tFLOAT\tINT\tFLOAT\tVARCHAR\(100\)\tVARCHAR\(100\)\tFLOAT\tVARCHAR\(100\)\tVARCHAR\(100\)\tFLOAT\tVARCHAR\(100\)\tFloat\tVARCHAR\(100\)\tINT\tFLOAT\tINT\tFLOAT\tINT\tFLOAT\tINT\tFLOAT\tINT\tINT\tINT\tFLOAT";
        print "Field\-type preference file generated\, may be edited as a tab-delimited text if needed\.\n";
        close PREFS;  #closes the preferences file
    }
    
    open (PREFS, "VCF2SQL.prefs.txt") or die "Unable to open preference file\.\n"; #opens the preferences file
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
            $printheadervalue =~ s/Phred_scaled_p_value_using_Fisher_s_exact_test_to_detect_strand_bias/Phred_scale_p_val_using_Fishers_exact_test_for_strand_bias/;
            $printheadervalue =~ s/Run_length__the_number_of_consecutive_repeats_of_the_alternate_allele_in_the_reference_genome/Run_len_number_of_consec_rpts_of_alt_allele_in_ref_genome/i;
            $printheadervalue =~ s/Inbreeding_coefficient_as_estimated_from_the_genotype_likelihoods_per_sample_when_compared_against_the_Hardy_Weinberg_expectation/Inbreed_coeff_est_from_geno_likelihood_agnst_the_HardyWeinberg/;
            $printheadervalue =~ s/Maximum_likelihood_expectation_MLE__for_the_allele_counts__not_necessarily_the_same_as_the_AC_for_each_ALT_allele__in_the_same_order_as_listed/MLE__for_the_allele_counts/;
            $printheadervalue =~ s/Maximum_likelihood_expectation_MLE__for_the_allele_frequency__not_necessarily_the_same_as_the_AF_for_each_ALT_allele__in_the_same_order_as_listed/MLE__for_the_allele_frequency/;
            $printheadervalue =~ s/This_variant_was_used_to_build_the_negative_training_set_of_bad_variants/Used_to_build_neg_training_set_of_bad_variants/;
            $printheadervalue =~ s/This_variant_was_used_to_build_the_positive_training_set_of_good_variants/Used_to_build_pos_training_set_of_good_variants/;
            $printheadervalue =~ s/the_//;
            
            if (length($printheadervalue) > 61) {  #if that didn't get the job done
                $printheadervalue = substr($printheadervalue, 0, 61 ); #use brute force and just chop it down to 61 characters
            }
        }
        print SQL $printheadervalue."VCF ".$prefhash{$headervalue}; #putting out a line of code for each one to create the approprate field.  Ending names with VCF to avoid any possible collisions for common names like uniLoc, chromosome, position, etc.
        if ($headers[$hlindex+1]) {
            print SQL "\,\n";
        }
        else {
            print SQL "\)\;";
        }
        $hlindex ++;
    }
    print SQL "LOAD DATA LOCAL INFILE \'$txtout\' INTO TABLE $tablename\nCOLUMNS TERMINATED BY \'\\t\'\nLINES TERMINATED BY \'\\n\'\nIGNORE 1 lines\;\n";  #tells it where to get the data and how to read it
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