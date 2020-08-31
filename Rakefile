require 'pry'
require 'leveldb'
require 'lexicon'

SPACE = ' '
UNDERSCORE = '_'
LETTERS = %w(a b c d e f g h i j k l m n o p q r s t u v w x y z)

# Files are broken up by first two letters of first word
LETTER_PAIRS = LETTERS.repeated_permutation(2).map(&:join)

# Ignore uses before this year.
MINIMUM_YEAR = 1900

# http://storage.googleapis.com/books/ngrams/books/datasetsv2.html
# > "Files with a letter followed by an underscore (e.g., s_) contain
# > ngrams that begin with the first letter, but have an unusual second
# > character.

# This includes single letter words like "a" and "I"
SINGLE_LETTERS = LETTERS.map{ |letter| "#{letter}_" }

FILES_TO_PARSE = (LETTER_PAIRS + SINGLE_LETTERS).map do |key|
  hash = {}
  hash[:key] = key

  hash[:filename] = "googlebooks-eng-all-2gram-20120701-#{key}"
  hash[:gzipped_filename] = "#{hash[:filename]}.gz"

  hash[:remote_gzipped_file_path] = "http://storage.googleapis.com/books/ngrams/books/#{hash[:gzipped_filename]}"

  hash[:local_gzipped_file_path] = "input/#{hash[:gzipped_filename]}"
  # Note: this is the path produced by `gunzip` with no arguments
  hash[:local_file_path] = "input/#{hash[:filename]}"

  # Marker files for running multiple instances
  hash[:started_marker_file] = "output/started/#{key}"
  hash[:output_file_path] = "output/complete/#{key}.marshal"

  hash
end

DB_PATH = "output/leveldb/bigrams.db"

# Files contain POS tagged words, untagged words (which appear to be the sum
# of all differnt POS uses), as well as standalone POS tag counts:
# Eg:
# red_ADJ hat ...
# red_ADJ hat_NOUN ...
# red_ADJ _NOUN_ ...
#POS_MARKERS = %w(_ADJ _ADP _ADV _CONJ _DET _NOUN _NUM _PRON _PRT _VERB _X _START _END _.)
#POS_MARKERS_REGEXP = Regexp.new(POS_MARKERS.map{ |s| Regexp.escape(s) }.join("|"))

namespace :bigrams do
  desc 'Run a pry debugger'
  task :pry do
    binding.pry
  end

  desc 'Create bigrams lookup from Google Books'
  task :create do
    FILES_TO_PARSE.each do |file|
      # Skip past started files
      next if File.file?(file[:started_marker_file])

      puts "Starting on '#{file[:key]}'..."

      # Mark the file as started
      File.write(file[:started_marker_file], Time.now.to_s)

      # Download the file from Google
      puts `wget -nc -O #{file[:local_gzipped_file_path]} #{file[:remote_gzipped_file_path]}`

      # Unzip the file.
      # This will result in a file at file[:local_file_path].
      # Gzipped version is automatically removed.
      puts `gunzip #{file[:local_gzipped_file_path]}`

      # Remove all lines with underscores
      puts `sed -i '/_/d' #{file[:local_file_path]}`

      processed_data = Hash.new

      line_count = `wc -l "#{file[:local_file_path]}"`.strip.split(SPACE)[0].to_i

      File.open(file[:local_file_path], "r") do |file_handle|
        file_handle.each_line do |line|
          # Print progress every 100,000 lines
          puts "#{file_handle.lineno} lines of '#{file[:key]}' processed (#{(file_handle.lineno / line_count.to_f * 100).round(2)}%)." if file_handle.lineno % 100000 == 0

          words, year, ngram_count, volume_count = line.split("\t")

          # Forget anything with an underscore left in it.
          # This removes all POS tagged words and standalone
          # POS tag bigrams.
          next if words[UNDERSCORE]

          # Convert counts and year to integers
          ngram_count = ngram_count.to_i
          volume_count = volume_count.to_i
          year = year.to_i

          next if year.to_i < MINIMUM_YEAR

          # Downcase words (mixes different uses of words)
          words.downcase!

          # Break words
          left_word, right_word = words.split(SPACE)

          # Forget stand-alone punctuation
          next unless [left_word, right_word].all?{ |w| w =~ /[a-z]/ }

          # Increment the counts
          processed_data[left_word.to_sym] ||= Hash.new
          processed_data[left_word.to_sym][right_word.to_sym] ||= Hash.new(0)
          processed_data[left_word.to_sym][right_word.to_sym][:match_count] += ngram_count
          processed_data[left_word.to_sym][right_word.to_sym][:volume_count] += volume_count
        end
      end

      # Store the Marshaled processed data
      File.open(file[:output_file_path], "w") do |f|
        Marshal.dump(processed_data, f)
      end

      # Delete the unzipped file
      File.delete(file[:local_file_path])

      print " done!"
    end
  end

  desc 'Store bigrams in leveldb'
  task :store_in_leveldb do
    puts "Storing in #{DB_PATH}..."
    db = LevelDB::DB.new DB_PATH

    total_word_count = 0
    FILES_TO_PARSE.each_with_index do |file, index|
      bigrams = File.open(file[:output_file_path], "r") { |f| Marshal.load(f) }

      total = bigrams.length
      i = 0
      bigrams.each do |word1, second_words|
        word1_count = 0
        second_words.each do |word2, counts|
          match_count = counts[:match_count]
          # Tally sum of all bigrams for word1
          word1_count += match_count

          # Store the bigram in levelDB
          db[[word1, word2].join("_")] = match_count.to_s
        end
        # Store the total word1 count in levelDB
        db["#{word1}__total"] = word1_count.to_s

        # Tally up all bigrams
        total_word_count += word1_count

        i += 1
        print "#{(i / total.to_f * 100).round(2)}% done     \r" if i % 10 == 0
        $stdout.flush
      end
      puts "Stored file #{index+1}/#{FILES_TO_PARSE.length} in levelDB."
    end
    db["__total"] = total_word_count.to_s
    db.close()
  end

  desc 'Store trimmed bigrams hash'
  task :trimmed_hash do
    puts "Storing in trimmed hash"
    dictionary = Lexicon.new
    hash = Hash.new { |h, left_word| h[left_word] = Hash.new }

    total_word_count = 0
    FILES_TO_PARSE.each_with_index do |file, index|
      bigrams = File.open(file[:output_file_path], "r") { |f| Marshal.load(f) }

      bigrams.each do |word1, second_words|
        word1 = word1.to_s
        # Skip non dictionary words
        next unless dictionary.lookup_insensitive(word1)

        word1_count = 0
        second_words.each do |word2, counts|
          word2 = word2.to_s
          # Skip non dictionary words
          next unless dictionary.lookup_insensitive(word2)

          match_count = counts[:match_count]
          # Tally sum of all bigrams for word1
          word1_count += match_count

          # Store the bigram in the master hash
          hash[word1][word2] = match_count
        end
        # Store the total word1 count
        hash[word1]["__total"] = word1_count

        # Tally up all bigrams
        total_word_count += word1_count
      end
      puts "Added file #{index+1}/#{FILES_TO_PARSE.length} (#{file[:key]}) to hash."
    end
    hash["__total"] = total_word_count.to_s
    # remove the default before storing with Marshal
    hash.default = nil
    File.open("bigrams_trimmed-dict.hash", "w") do |f|
      Marshal.dump(hash, f)
    end
    puts "Done"
  end

  desc "Download and unzup 1grams"
  task :fetch_1grams do
    data = Hash.new(0)

    LETTERS.each do |letter|
      filename = "googlebooks-eng-all-1gram-20120701-#{letter}"
      gz_filename = "#{filename}.gz"

      gz_remote_file_path = "http://storage.googleapis.com/books/ngrams/books/#{gz_filename}"

      local_file_path = "1grams/#{filename}"
      gz_local_file_path = "#{local_file_path}.gz"

      # Download and unzip the file if required
      if !File.exists?(local_file_path)
        if !File.exists?(gz_local_file_path)
          puts `wget -nc -O #{gz_local_file_path} #{gz_remote_file_path}`
        end
        puts `gunzip #{gz_local_file_path}`
      end

      line_count = `wc -l "#{local_file_path}"`.strip.split(SPACE)[0].to_i
      File.open(local_file_path, "r") do |fh|
        fh.each_line do |line|
          # Print progress every 10,000 lines
          puts "#{fh.lineno} lines of '#{letter}' processed (#{(fh.lineno / line_count.to_f * 100).round(2)}%)." if fh.lineno % 10_000 == 0

          word, year, ngram_count, _volume_count = line.split("\t")

          # Convert counts and year to integers
          ngram_count = ngram_count.to_i
          year = year.to_i

          # Ignore old texts
          next if year.to_i < MINIMUM_YEAR

          # Add this year's count to the total
          data[word] += ngram_count
        end
      end

      # Delete the unzipped file
      File.delete(local_file_path)
    end
    File.open("1grams/1grams.hash", "w") do |f|
      Marshal.dump(data, f)
    end
  end
end
