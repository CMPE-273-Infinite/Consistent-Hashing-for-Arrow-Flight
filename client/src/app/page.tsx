'use client'
import * as React from 'react';
import Box from '@mui/material/Box';

export default function DrawerAppBar() {

  return (
    <Box sx={{ display: 'flex' }}>
      <Box component="main" sx={{ p: 3 }}>
        Select Role
      </Box>
    </Box>
  );
}
