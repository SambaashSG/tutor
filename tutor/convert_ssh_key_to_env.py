def convert_key_input_to_env(output_file, env_var_name="SSH_PRIVATE_KEY"):
    print("Paste your SSH private key below. When done, type 'EOF' on a new line and press Enter:")

    lines = []
    while True:
        line = input()
        if line.strip() == "EOF":
            break
        lines.append(line)

    key_content = "\n".join(lines).strip()
    single_line_key = key_content.replace('\n', '\\n')

    with open(output_file, 'a') as f:
        f.write(f'{env_var_name}={single_line_key}\n')

    print(f"SSH private key has been saved to {output_file} as {env_var_name}")

if __name__ == "__main__":
    output = input("Enter the path to the .env output file: ").strip()
    var_name = input("Enter the environment variable name (default: SSH_PRIVATE_KEY): ").strip()
    if not var_name:
        var_name = "SSH_PRIVATE_KEY"

    convert_key_input_to_env(output, var_name)
