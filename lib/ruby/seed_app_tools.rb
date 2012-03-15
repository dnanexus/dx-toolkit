require "nucleus.rb"

# convert name to int by interpretting it as a base-256 number
def chr_name_to_id(name)
  id = 0
  multiple = 1
  name.each_char do |character|
    id += character.ord * multiple
    multiple *= 256
  end
  return (id % 1000000000)
end

# Maximum number of reads to send in each call to the
# append-table-column-set method.
READS_PER_CHUNK = 1000
ROWS_PER_CHUNK = 1000
UNMAPPED_CHR = chr_name_to_id("nomap")

module SeedAppTools
  def SeedAppTools.uploadFile(nuc, source, content_type)
    location = nuc.create_upload(1)
    nuc.put_upload(location["parts"][0], source)
    file_id = nuc.create_file({"id" => location["id"], "content_type" => content_type}.to_json)
    return file_id
  end

  # code from migrate-pipeline (fastq2table) with modification to nucleas API
  def SeedAppTools.fastq2table(nuc, fastq_file)
    fastq_columns = ['name:string', 'seq:string', 'qual:string'].to_json
    table_id = nuc.create_table(fastq_columns)

    line_index, read_index = 0, 0
    name, seq, qual = nil, nil, nil
    rows = []

    File.open(fastq_file).each do|line|
      line.strip!

      if line_index == 0
	 name = line[1, line.length-1]
      elsif line_index == 1
	 seq = line
      elsif line_index == 3
	 qual = line
	 rows << [name, seq, qual]
      end
      
      line_index = (line_index + 1) % 4
      
      if rows.size == READS_PER_CHUNK
	 read_index += rows.size
	 nuc.append_rows(table_id, "", rows)
	 rows = []
      end
    end 
    
    if !rows.empty?
      read_index += rows.size
      nuc.append_rows(table_id, "", rows)
    end
    
    raise "Error when close table" if ! nuc.close_table(table_id)
    
    return JSON.generate( { "table_id"=>table_id, "num_rows"=>read_index } )
  end

  # code from migrate-pipeline (table2fastq) with modification to nucleas API
  def SeedAppTools.table2fastq(nuc, table_id, fastq_file, offset=nil, count=nil)
    table_hash = nuc.get_table(table_id)
    raise "table is not closed: #{table_id}" if table_hash["status"] != "CLOSED"

    if offset.nil?
      offset = 0
      count = table_hash["numRows"].to_i
    end

    raise "offset must be non-negative: #{offset}" if offset < 0
    raise "offset too large: #{offset}, num_rows: #{table_hash["numRows"]}" if offset > table_hash["numRows"].to_i
    raise "count must be non-negative: #{count}" if count < 0
    
    count = table_hash["numRows"].to_i - offset if (count + offset) > table_hash["numRows"].to_i

    row_index = offset
    File.open(fastq_file, "w") do |file|
      while row_index < offset + count
	 query_count = [READS_PER_CHUNK, offset + count - row_index].min
	 rows = nuc.get_rows("tables", table_id, ["name", "seq", "qual"], row_index, query_count)
	 rows.each do |row|
	   name, seq, qual = row
	   file.write("@#{name}\n#{seq}\n+#{name}\n#{qual}\n")			   
	 end
	 row_index += query_count	
      end
    end

    return JSON.generate( {"fastq_file"=>fastq_file, "num_rows"=>row_index} )
  end

  # code from migrate-pipeline (sam2coordstable) with modification to nucleas API
  def SeedAppTools.sam2coordstable(nuc, sam_file, cs_name)
    columns = ['chr:int32', 'start:int32', 'stop:int32', 'sam:string'].to_json
    # first store data into a table
    table_id = nuc.create_table(columns)

    header, read_index, rows = "", 0, []
    File.open(sam_file).each do |line|
      line.strip!

      if line[0] == '@'
	 # header line: put in header, to be stored as a object property
	 header += line + "\n"
      else
	 # alignment line: put into table
	 line_split = line.split("\t")
	 flags = Integer(line_split[1])
	 unmapped = flags & 0x4
	 if unmapped
	   chr, start, stop = UNMAPPED_CHR, 0, 0
	 else
	   chr = chr_name_to_id(line_split[2])
	   start = Integer(line_split[3])
	   length = Integer(line_split[8])
	   stop = start + length
	 end

	 rows << [chr, start, stop, line]
      end
      
      if rows.size == READS_PER_CHUNK
	 read_index += rows.size
	 raise "Row numbers do not match" if read_index != nuc.append_rows(table_id, "", rows)
	 rows = []
      end
    end
    
    if !rows.empty?
      read_index += rows.size
      raise "Row numbers do not match" if read_index != nuc.append_rows(table_id, "", rows)
    end
    
    raise "Error when close table" if ! nuc.close_table(table_id)

    table_id2 = nuc.create_coordstable(table_id)
    header = header.to_json
    nuc.set_property("coords_tables", table_id2, "#{cs_name}_sam_header", header)
    return JSON.generate( { "table_id"=>table_id, "coordstable_id" => table_id2, "num_rows"=>read_index} )
  end

  # code from migrate-pipeline (coordstable2sam) with modification to nucleas API
  def SeedAppTools.coordstable2file(nuc, table_id, header_name, column_name, filename, offset, count)
    table_hash = nuc.get_coordstable(table_id)
    raise "table is not closed: #{table_id}" if table_hash["status"] != "CLOSED"

    if offset.nil?
      offset = 0
      count = table_hash["numRows"].to_i
    end

    raise "offset must be non-negative: #{offset}" if offset < 0
    raise "offset too large: #{offset}, num_rows: #{table_hash["numRows"]}" if offset > table_hash["numRows"].to_i
    raise "count must be non-negative: #{count}" if count < 0
	
    header = nuc.get_property('coords_tables', table_id, header_name)
    row_index = offset

    File.open(filename, "w") do |file|
      file.write header unless header.nil?
      while row_index < offset + count
	 query_count = [ROWS_PER_CHUNK, offset + count - row_index].min
	 rows = nuc.get_rows("coords_tables", table_id, [column_name], row_index, query_count)
	 file.puts(rows.join("\n"))
	 row_index += query_count
	 file.write("\n")
      end
    end
    
    return JSON.generate({"num_rows" => row_index})
  end

  def SeedAppTools.coordstable2sam(nuc, table_id, sam_file, cs_name = nil, offset = nil, count = nil)
    cs_name = "sam_alignment" if cs_name.nil?
    return SeedAppTools.coordstable2file(nuc, table_id, "#{cs_name}_sam_header", "sam", sam_file, offset, count); 
  end

  # code from migrate-pipeline (vcf2coordstable) with modification to nucleas API
  def SeedAppTools.vcf2coordstable(nuc, vcf_file, cs_name)
    cs_name = 'variants' if cs_name.nil? 
    columns = ['chr:int32', 'start:int32', 'stop:int32', 'vcf:string'].to_json
    table_id = nuc.create_table(columns)

    header, rows, row_index = "", [], 0
    File.open(vcf_file).each do |line|
      line.strip!
      if line[0] == '#'
	 # header line: put in header, to be stored as a object property
	 header += line
      else
	 # regular line: put into table
	 chr, pos, id, ref = line.split("\t", -1)
	 
	 chr = chr_name_to_id(chr)
	 start = pos.to_i - 1
	 stop = start + ref.length
	 
	 rows << [chr, start, stop, line]

	 if rows.size == ROWS_PER_CHUNK  
	   row_index += rows.size
	   raise "Row numbers do not match" if row_index != nuc.append_rows(table_id, "", rows)
	   rows = []
	 end
      end
    end
    
    if !rows.empty?
      row_index += rows.size
      raise "Row numbers do not match" if row_index != nuc.append_rows(table_id, "", rows)
    end

    raise "Error when close table" if ! nuc.close_table(table_id)
    
    table_id2 = nuc.create_coordstable(table_id)

    header = header.to_json
    nuc.set_property("coords_tables", table_id2, "#{cs_name}_vcf_header", header)
    return JSON.generate( { "table_id"=>table_id, "coordstable_id" => table_id2, "num_rows"=>row_index} )  
  end
    
  def SeedAppTools.coordstable2vcf(nuc, table_id, vcf_file, cs_name = nil, offset = nil, count = nil)
    cs_name = "variants" if cs_name.nil?
    return SeedAppTools.coordstable2file(nuc, table_id, "#{cs_name}_vcf_header", "vcf", vcf_file, offset, count); 
  end

=begin
  # code from migrate-pipeline (generic2coordstable) with modification to nucleas API
  def SeedAppTools.generic2coordstable(nuc, file, chr_index, start_index, end_index, data_fileds)
    columns = [['chr', 'integer'], ['start', 'integer'], ['end', 'integer'], ['gene_name', 'string'], ['gene_strand', 'string']].to_json
    table_id = nuc.create_coordstable(columns)

    header = nil
    rows = []
    row_index = 0
    File.open(file) do |io|
      io.each_line do |line|
	 if line[0] == '#'
	   # header line: put in header, to be stored as a object property
	   header ||= ""
	   header += line
	 else
	   # regular line: put into table
	   fields = line.strip.split("\t", -1)
	   
	   row = [chr_name_to_id(fields[chr_index]), fields[start_index].to_i, fields[end_index].to_i]
	   row.concat(data_fields.map { |i| fields[i] })
	   
	   rows << row

	   if rows.size == ROWS_PER_CHUNK
	     $stderr.puts "Appending row data..."
	     nuc.append_table_column_set(table_id, cs_name, row_index, rows)
	     $stderr.puts "  Done."
	     row_index += rows.size
	     rows = []
	   end
	 end
      end
    end

    if !rows.empty?
      $stderr.puts "Appending final row data..."
      nuc.append_table_column_set(table_id, cs_name, row_index, rows)
      $stderr.puts "  Done."
    end

    $stderr.puts "Setting header as object property..."
    header = header.to_json
    $stderr.puts "Writing header: " + header
    nuc.set_property(table_id, "#{cs_name}_header", header)
    $stderr.puts "  Property set."

    $stderr.puts "Closing column set #{cs_name}..."
    nuc.close_table_column_set(table_id, cs_name)
    $stderr.puts "  Closed."
  end
=end
end
