from src.channel_list_import import load_channel_list_from_file


def test_load_channel_list_from_file_parses_unique_channels(tmp_path):
    channel_file = tmp_path / "channels.txt"
    channel_file.write_text(
        """
        # Comment line
        @primarychannel
        backupchannel
        https://t.me/example_feed
        -1001234567890
        https://t.me/example_feed
        """.strip(),
        encoding="utf-8",
    )

    result = load_channel_list_from_file(channel_file)

    assert result.channels == [
        "@primarychannel",
        "@backupchannel",
        "https://t.me/example_feed",
        "-1001234567890",
    ]
    assert result.duplicate_entries == ["https://t.me/example_feed"]
    assert result.invalid_entries == []
    assert result.encoding_errors is False


def test_load_channel_list_from_file_reports_invalid_rows(tmp_path):
    channel_file = tmp_path / "invalid.txt"
    channel_file.write_text(
        """
        not-a-channel
        https://example.com/not-telegram
        """.strip(),
        encoding="utf-8",
    )

    result = load_channel_list_from_file(channel_file)

    assert result.channels == []
    assert len(result.invalid_entries) == 2
    assert "Line 1" in result.invalid_entries[0]
    assert "Line 2" in result.invalid_entries[1]


def test_load_channel_list_from_file_supports_scheme_less_urls(tmp_path):
    channel_file = tmp_path / "scheme_less.txt"
    channel_file.write_text("t.me/ExampleChannel\n", encoding="utf-8")

    result = load_channel_list_from_file(channel_file)

    assert result.channels == ["https://t.me/ExampleChannel"]


def test_load_channel_list_from_file_handles_encoding_errors(tmp_path):
    channel_file = tmp_path / "encoding.txt"
    # Write bytes that are not valid UTF-8 to trigger the fallback path.
    channel_file.write_bytes("канал".encode("cp1251"))

    result = load_channel_list_from_file(channel_file)

    assert result.encoding_errors is True
    assert result.channels == []
    assert result.invalid_entries  # Replacement characters lead to invalid identifiers

