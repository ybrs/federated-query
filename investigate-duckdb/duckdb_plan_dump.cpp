#include "duckdb.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

using duckdb::Connection;
using duckdb::DuckDB;
using duckdb::LogicalOperator;
using duckdb::string;

struct Arguments {
	string database = ":memory:";
	string setup_file;
	string sql_file;
};

// Reads a complete text file into memory.
static string ReadFile(const string &path) {
	std::ifstream input(path);
	if (!input) {
		throw std::runtime_error("Could not open file: " + path);
	}
	std::stringstream buffer;
	buffer << input.rdbuf();
	return buffer.str();
}

// Escapes a string so it can be emitted inside JSON output.
static string JsonEscape(const string &value) {
	string result;
	for (auto character : value) {
		switch (character) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			result += character;
		}
	}
	return result;
}

// Writes one JSON string literal to the output stream.
static void WriteJsonString(std::ostream &output, const string &value) {
	output << "\"" << JsonEscape(value) << "\"";
}

// Joins one logical operator's expression names or SQL strings.
static string JoinExpressionText(const LogicalOperator &op, bool use_names) {
	string result;
	for (duckdb::idx_t expression_index = 0; expression_index < op.expressions.size(); expression_index++) {
		if (expression_index > 0) {
			result += "\n";
		}
		if (use_names) {
			result += op.expressions[expression_index]->GetName();
		} else {
			result += op.expressions[expression_index]->ToString();
		}
	}
	return result;
}

// Writes DuckDB display metadata plus full expression strings for one operator.
static void WriteExtraInfo(std::ostream &output, const LogicalOperator &op) {
	output << "{";
	bool first_entry = true;
	auto params = op.ParamsToString();
	for (auto &entry : params) {
		if (!op.expressions.empty() && entry.first == "Expressions") {
			continue;
		}
		if (!first_entry) {
			output << ",";
		}
		WriteJsonString(output, entry.first);
		output << ":";
		WriteJsonString(output, entry.second);
		first_entry = false;
	}
	if (!op.expressions.empty()) {
		if (!first_entry) {
			output << ",";
		}
		WriteJsonString(output, "Expression Names");
		output << ":";
		WriteJsonString(output, JoinExpressionText(op, true));
		output << ",";
		WriteJsonString(output, "Expressions");
		output << ":";
		WriteJsonString(output, JoinExpressionText(op, false));
	}
	output << "}";
}

// Writes one logical operator and all children as JSON.
static void WritePlanNode(std::ostream &output, const LogicalOperator &op) {
	output << "{";
	WriteJsonString(output, "name");
	output << ":";
	WriteJsonString(output, op.GetName());
	output << ",";
	WriteJsonString(output, "children");
	output << ":[";
	for (duckdb::idx_t child_index = 0; child_index < op.children.size(); child_index++) {
		if (child_index > 0) {
			output << ",";
		}
		WritePlanNode(output, *op.children[child_index]);
	}
	output << "],";
	WriteJsonString(output, "extra_info");
	output << ":";
	WriteExtraInfo(output, op);
	output << "}";
}

// Reads the value following a command-line flag.
static string ReadArgumentValue(int argc, char **argv, int &argument_index, const string &argument) {
	if (argument_index + 1 >= argc) {
		throw std::runtime_error("Missing value for argument: " + argument);
	}
	argument_index++;
	return argv[argument_index];
}

// Parses command-line arguments for the plan dump helper.
static Arguments ParseArguments(int argc, char **argv) {
	Arguments arguments;
	for (int argument_index = 1; argument_index < argc; argument_index++) {
		string argument = argv[argument_index];
		if (argument == "--database") {
			arguments.database = ReadArgumentValue(argc, argv, argument_index, argument);
		} else if (argument == "--setup-file") {
			arguments.setup_file = ReadArgumentValue(argc, argv, argument_index, argument);
		} else if (argument == "--sql-file") {
			arguments.sql_file = ReadArgumentValue(argc, argv, argument_index, argument);
		} else {
			throw std::runtime_error("Unknown argument: " + argument);
		}
	}
	if (arguments.sql_file.empty()) {
		throw std::runtime_error("--sql-file is required");
	}
	return arguments;
}

// Builds an optimized logical plan and writes it as enriched JSON.
int main(int argc, char **argv) {
	try {
		auto arguments = ParseArguments(argc, argv);
		DuckDB database(arguments.database);
		Connection connection(database);
		if (!arguments.setup_file.empty()) {
			auto setup_result = connection.Query(ReadFile(arguments.setup_file));
			if (setup_result->HasError()) {
				setup_result->ThrowError();
			}
		}
		auto plan = connection.ExtractPlan(ReadFile(arguments.sql_file));
		std::cout << "[";
		WritePlanNode(std::cout, *plan);
		std::cout << "]\n";
		return 0;
	} catch (std::exception &error) {
		std::cerr << error.what() << "\n";
		return 1;
	}
}
