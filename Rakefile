require 'leveldb'
require 'pry'
SPACE = ' '
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

  hash[:local_file_path] = "input/#{hash[:filename]}"
  hash[:local_gzipped_file_path] = "input/#{hash[:gzipped_filename]}"

  # Marker files for running multiple instances
  hash[:started_marker_file] = "output/started/#{key}"
  hash[:output_file_path] = "output/complete/#{key}.marshal"

  hash
end

# Files contain POS tagged words, as well as POS counts:
# Eg:
# red_ADJ hat_NOUN ...
# red_ADJ _NOUN_ ...
POS_MARKERS = %w(_ADJ _ADP _ADV _CONJ _DET _NOUN _NUM _PRON _PRT _VERB _X _START _END _.)
POS_MARKERS_REGEXP = Regexp.new(POS_MARKERS.map{ |s| Regexp.escape(s) }.join("|"))

namespace :bigrams do
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

      # Unzip the file. Gzipped version is automatically removed.
      puts `gunzip #{file[:local_gzipped_file_path]} > #{file[:local_file_path]}`

      processed_data = Hash.new

      line_count = `wc -l "#{claimed_input_file}"`.strip.split(SPACE)[0].to_i

      File.open(claimed_input_file, "r") do |file_handle|
        file_handle.each_line do |line|
          # Print progress every 100,000 lines
          puts "#{file_handle.lineno} lines processed (#{(file_handle.lineno / line_count.to_f * 100).round(2)}%)." if file_handle.lineno % 100000 == 0

          words, year, ngram_count, volume_count = line.split("\t")

          next if year.to_i < MINIMUM_YEAR

          # Convert to lowercase and break words
          left_word, right_word = words.split(SPACE)

          # Remove POS annotations (mix different uses of words)
          # This also mames all POS placeholders (eg: _ADV_) into "_"
          right_word.gsub!(POS_MARKERS_REGEXP, '')
          left_word.gsub!(POS_MARKERS_REGEXP, '')

          # Forget anything with an underscore left in it
          next if [left_word, right_word].any?{ |w| w =~ /_/ }

          # Forget about upper/lower (mix different uses of words more)
          left_word.downcase!
          right_word.downcase!

          # Forget stand-alone punctuation
          next unless [left_word, right_word].all?{ |w| w =~ /[a-z]/ }

          processed_data[left_word.to_sym] ||= Hash.new(0)
          processed_data[left_word.to_sym][right_word.to_sym] ||= Hash.new(0)
          processed_data[left_word.to_sym][right_word.to_sym][:match_count] += ngram_count.to_i
          processed_data[left_word.to_sym][right_word.to_sym][:volumn_count] += volume_count.to_i
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
    output_file_path = "joined.marshal"
    bigrams = File.open(output_file_path, "r") { |file| Marshal.load(file) }

    db_path = "db/bigrams-leveldb.db"
    print "Storing in #{db_path}..."
    db = LevelDB::DB.new db_path

    total = bigrams.length
    i = 0
    bigrams.each do |word1, hash|
      hash.each do |word2, count|
        key = [word1, word2].join("_")
        db[key] = count.to_s
      end
      i += 1
      puts "#{(i / total.to_f * 100).round(2)}% done"
    end
    puts 'finished.'
  end
  desc 'Store bigrams totals in leveldb'
  task :store_totals_in_leveldb do
    output_file_path = "joined.marshal"
    bigrams = File.open(output_file_path, "r") { |file| Marshal.load(file) }

    db_path = "db/bigrams-leveldb.db"
    print "Storing in #{db_path}..."
    db = LevelDB::DB.new db_path

    total = bigrams.length
    i = 0
    total_count = 0
    bigrams.each do |word1, hash|
      word_count = hash.values.inject(:+)
      total_count += word_count
      db["#{word1}__total"] = word_count.to_s

      i += 1
      puts "#{(i / total.to_f * 100).round(2)}% done" if i % 100 == 0
    end
    db["__total"] = total_count.to_s
    puts 'finished.'
  end
end
