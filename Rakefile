require 'pry'
SPACE = ' '
LETTERS = %w(a b c d e f g h i j k l m n o p q r s t u v w x y z)
LETTER_PAIRS = LETTERS.repeated_permutation(2).map(&:join)

namespace :bigrams do
  desc "Remove bigrams from files where they don't belong. Fix my mistakes."
  task :clean_marshal do
    LETTER_PAIRS.each do |ll|
      output_file_path = "marshal/google-bigrams-#{ll}.marshal"
      next unless File.file?(output_file_path)
      puts "Cleaning #{output_file_path}"
      h = File.open(output_file_path, 'r') do |file|
        Marshal.load(file)
      end

      count = h.length
      # Exclude anything not stating with `ll`
      h.delete_if{|k,v| !(k.to_s =~ Regexp.new("^"+ll)) }

      puts "#{count - h.length} bigrams removed"
      puts "#{h.length} bigrams retained"
      File.open(output_file_path.gsub('.marshal', '.clean.marshal'), "w") do |file|
        Marshal.dump(h, file)
      end
      puts "-------"
    end
  end

  desc 'Download gzipped bigram data files from Google Books'
  task :maintain_input_files do
    operation_completed = false
    until operation_completed do
      operation_completed = true
      current_count = Dir[File.join('originals', '**', '*')].count { |file| file =~ /googlebooks-eng-all-2gram-20120701-[a-z][a-z]/ }
      if current_count >= 4
        puts "There are already #{current_count} downloaded files. Waiting..."
        operation_completed = false
        sleep 5
        next
      end
      LETTERS.repeated_permutation(2).each do |ll|
        ll = ll.join
        output_file_path = "marshal/google-bigrams-#{ll}.marshal"
        input_file_name = "googlebooks-eng-all-2gram-20120701-#{ll}.gz"
        input_file_path = "originals/#{input_file_name}"

        if File.file?(output_file_path) 
          puts "Already procesed #{ll}. Skipping."
          next
        elsif Dir[File.join('originals', '**', '*')].count { |file| file =~ /googlebooks-eng-all-2gram-20120701-#{ll}/ } > 0
          puts "Already downloaded #{ll}. Skipping."
          next
        else
          path = "http://storage.googleapis.com/books/ngrams/books/#{input_file_name}"
          puts "Downloading #{path}"
          # Download (with -nc just in case)
          
          puts `wget -nc -O #{input_file_path} #{path}`
          # Unzip
          puts `gunzip #{input_file_path}`

          operation_completed = false
          break
        end
      end
    end
    puts "Downloading is complete. All files process or downloaded."
  end

  desc 'Create bigrams lookup from Google Books'
  task :create do |t, args|

    pos_markers = %w(_ADJ _ADP _ADV _CONJ _DET _NOUN _NUM _PRON _PRT _VERB _X _START _END _.)
    pos_indicators = Regexp.new(pos_markers.map{ |s| Regexp.escape(s) }.join("|"))

    operation_completed = false

    until operation_completed do
      if Dir[File.join('originals', '**', '*')].count { |file| file =~ /googlebooks-eng-all-2gram-20120701-[a-z][a-z]$/ } == 0
        puts "Waiting for an input file... try again in 20 seconds."
        sleep 20
      end

      operation_completed = true

      LETTERS.repeated_permutation(2).each do |ll|
        ll = ll.join
        input_file = "originals/googlebooks-eng-all-2gram-20120701-#{ll}"
        output_file_path = "marshal/google-bigrams-#{ll}.marshal"

        # Skip if we've done ths file already
        if File.file?(output_file_path)
          puts "Already generated '#{ll}.marshal'. Skipping."
          next
        else
          operation_completed = false
        end

        if File.file?(input_file)
          if File.file?(input_file+'gz')
            puts "Waiting for #{input_file}.gz to complete"
            next
          end
          puts "Creating bigrams from '#{ll}'..."
          claimed_input_file = input_file+".taken"
          # Rename the file to claim it
          File.rename(input_file, claimed_input_file)
        else
          puts "No input file found for '#{ll}'. Skipping."
          next
        end

        hash = Hash.new

        line_count = `wc -l "#{claimed_input_file}"`.strip.split(' ')[0].to_i

        File.open(claimed_input_file, "r") do |file_handle|
          file_handle.each_line do |line|
            if file_handle.lineno % 100000 == 0
              puts "#{file_handle.lineno} lines processed (#{(file_handle.lineno / line_count.to_f * 100).round(2)}%)."
            end

            words, _year, count, _book_count = line.split("\t")

            # Convert to lowercase and break words
            left_word, right_word = words.split(SPACE)

            # Remove POS annotations (mix different uses of words)
            # This also mames all POS placeholders (eg: _ADV_) into "_"
            right_word.gsub!(pos_indicators, '')
            left_word.gsub!(pos_indicators, '')

            # Forget anything with an underscore left in it
            next if [left_word, right_word].any?{ |w| w =~ /_/ }

            # Forget about upper/lower (mix different uses of words more)
            left_word.downcase!
            right_word.downcase!

            # Forget stand-alone punctuation
            next unless [left_word, right_word].all?{ |w| w =~ /[a-z]/ }

            hash[left_word.to_sym] ||= Hash.new(0)
            hash[left_word.to_sym][right_word.to_sym] += count.to_i
          end  
        end

        File.open(output_file_path, "w") do |file|
          Marshal.dump(hash, file)
        end

        print " done."
        File.delete(claimed_input_file)
        puts "Deleted: #{claimed_input_file}"
      end
    end
    puts "All Marshal files complete!"
  end
end
