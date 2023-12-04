'use client'
import AppBar from '@mui/material/AppBar';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import Toolbar from '@mui/material/Toolbar';
import Typography from '@mui/material/Typography';
import { useRouter } from 'next/navigation'
import React, { useState } from 'react';
import { Button, Container, TextField } from '@mui/material';

export default function User() {
  const [file, setFile] = useState(null);
  const handleFileChange = (event: any) => {
    // Access the selected file from the event
    const selectedFile = event.target.files[0];
    setFile(selectedFile);
  };

  const handleSubmit = async (event: any) => {
    event.preventDefault();
    // Handle form submission, e.g., send the file to the server
    if (file) {
      // Perform your upload logic here
      const formData = new FormData();
      formData.append('file', file);
      const response = await fetch('http://127.0.0.1:8000/upload-csv/', {
        method: 'POST',
        body: formData,
      });

      console.log('Uploading file:', file);
    } else {
      console.error('No file selected');
    }
  };
  return (
    <Box sx={{ display: 'flex' }}>
      <div>User</div>
      <Container>
        <form onSubmit={handleSubmit}>
          <input
            type="file"
            accept=".csv, .xlsx, .txt" // Define accepted file types
            onChange={handleFileChange}
            style={{ display: 'none' }}
            id="file-input"
          />
          <label htmlFor="file-input">
            <Button
              variant="contained"
              component="span"
              color="primary"
              fullWidth
            >
              Upload File
            </Button>
          </label>
          <Button
            type="submit"
            variant="contained"
            color="primary"
            fullWidth
          >
            Submit
          </Button>
        </form>
      </Container>

    </Box>
  );
}
