# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name          = 'polipus-cassandra'
  spec.version       = '0.0.1'
  spec.authors       = ['Stefano Fontanelli']
  spec.email         = ['s.fontanelli@gmail.com']
  spec.summary       = 'Add support for Cassandra in Polipus crawler'
  spec.description   = 'Add support for Cassandra in Polipus crawler'
  spec.homepage      = 'https://github.com/stefanofontanelli/polipus-cassandra'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(/^bin\//) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(/^(test|spec|features)\//)
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'polipus', '~> 0.3', '>= 0.3.0'
  spec.add_runtime_dependency 'cassandra-driver', '~> 2.1.1', '>= 2.1.1'

  spec.add_development_dependency 'rake'
end
