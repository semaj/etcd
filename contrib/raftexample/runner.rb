`rm -rf raftexample-* && go build`
N = ARGV[0].chomp.to_i
ports = N.times.map { |i| 8080 + i + 1 }
cluster = ports.map do |port|
  "http://127.0.0.1:#{port}"
end.join(",")

threads = []
N.times do |i|
  i = i + 1
  threads << Thread.new do
    output = `./raftexample --id #{i} --cluster #{cluster} 2> /dev/null`
    puts output.split("\n").find { |l| l.match(/BLAST:.+/) }[6..-1]
  end
end
threads.map(&:join)
