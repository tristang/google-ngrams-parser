SPACE = ' '
namespace :create do
  desc 'Create bigrams lookup from Google Books'
  task :google_bigrams do
    hash = Hash.new

    pos_markers = %w(_ADJ _ADP _ADV _CONJ _DET _NOUN _NUM _PRON _PRT _VERB _X _START _END _.)
    pos_indicators = Regexp.new(pos_markers.map{ |s| Regexp.escape(s) }.join("|"))

    letters = %w(a  b c d e f g h i j k l m n o p q r s t u v w x y z)
    letters.repeated_permutation(2).each do |ll|
      ll = ll.join
      path = "originals/googlebooks-eng-all-2gram-20120701-#{ll}"
      unless File.file?(path)
        puts "No file found for '#{ll}'. Skipping."
        next
      else
        print "Creating bigrams from '#{ll}'..."
      end

      File.open(path, "r") do |file_handle|
        file_handle.each_line do |line|
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

      File.open("marshal/google-bigrams-#{ll}.marshal", "w") do |file|
        Marshal.dump(hash, file)
      end

      puts " done."
    end
  end
end
